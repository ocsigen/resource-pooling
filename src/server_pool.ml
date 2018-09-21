(**
This module is built around [Resource_pool]. While a pool of type
[Resource_pool.t] manages a number of resources, here we manage a cluster of
such pools. A typical use case would be a cluster of servers, where for each
server we maintain a number of connections. A user of this module can call [use]
to access one of the connections, which are served in a round-robin fashion.
*)

[@@@ocaml.warning "+A-44-48"]

let (>>=) = Lwt.(>>=)

let section = Lwt_log.Section.make "server-pool"
let () = Lwt_log.Section.set_level section Lwt_log.Info


module type CONF = sig
  type connection
  type server
  type serverid
  val serverid_to_string : serverid -> string (** for debugging info *)
  val connect : server -> connection Lwt.t
  val close : connection -> unit Lwt.t
end

module Make (Conf : CONF) = struct

  type connection_pool = {
    serverid : Conf.serverid;
    connections : Conf.connection Resource_pool.t
  }

  type server_status = {
    desired : int;
    current : int
  }

  (* Each server holds its own connection_pool, so a server pool is a pool of
     connection pools. HOWEVER, [server_pool] will not contain one
     connection pool per server, but [n] times the same connection pool per
     server, where [n] is the (maximum) size of the servers connection pool. *)
  let server_pool : connection_pool Resource_pool.t =
    let nil () = failwith "Bs_db.server_pool: invalid connection attempt" in
    (* We supply [0] as the first argument to [Resource_pool.create] as it will
       prevent [Resource_pool] to ever create a new resource on its own. This is what
       we want since new servers are to be added by the user of this module. *)
    Resource_pool.create 0 nil

  let servers : (Conf.serverid, server_status) Hashtbl.t = Hashtbl.create 9

  let log_server_status {desired; current} =
    Lwt_log.ign_info_f ~section "current number of connections: %n/%n"
                                current desired
  let update_current_count serverid f =
    let server_status =
      let old_status = Hashtbl.find servers serverid in
      {old_status with current = f old_status.current}
    in
    Hashtbl.replace servers serverid server_status;
    log_server_status server_status

  let server_exists serverid = Hashtbl.mem servers serverid

  let add_many ?(connect_immediately = false) ~num_conn new_servers =
    let mk_connection_pool (serverid, server) : connection_pool =
      Lwt_log.ign_notice_f ~section "adding server: %s"
                                    (Conf.serverid_to_string serverid);
      let connect () =
        Lwt_log.ign_info_f ~section "opening connection to server %s"
                                    (Conf.serverid_to_string serverid);
        update_current_count serverid succ;
        Conf.connect server
      in
      let dispose conn =
        Lwt_log.ign_info_f ~section
          "closing connection to server %s" (Conf.serverid_to_string serverid);
        update_current_count serverid pred;
        Lwt.catch (fun () -> Conf.close conn) (fun _ -> Lwt.return_unit)
      in
      let server_status = {desired = num_conn; current = 0} in
      Hashtbl.add servers serverid server_status;
      let conn_pool = Resource_pool.create num_conn ~dispose connect in
      if connect_immediately then
        for _ = 1 to num_conn do
          Lwt.async @@ fun () ->
            connect () >>= fun c ->
            try Resource_pool.add conn_pool c; Lwt.return_unit
            with Resource_pool.Resource_limit_exceeded -> dispose c
        done;
      {serverid; connections = conn_pool}
    in
    let pools = List.map mk_connection_pool @@
      List.filter (fun l -> not @@ server_exists @@ fst l) new_servers in
    for _ = 1 to num_conn do
      List.iter (Resource_pool.add ~omit_max_check:true server_pool) pools
    done

  let add_one ?connect_immediately ~num_conn serverid server =
    add_many ?connect_immediately ~num_conn [(serverid, server)]

  let add_existing ~num_conn serverid connections =
    Lwt_log.ign_notice_f ~section "adding existing server: %s"
                                  (Conf.serverid_to_string serverid);
    let server_status = {desired = num_conn; current = 0} in
    Hashtbl.add servers serverid server_status;
    for _ = 1 to num_conn do
      Resource_pool.add ~omit_max_check:true server_pool {serverid; connections}
    done

  let remove serverid =
    Lwt_log.ign_notice_f ~section "removing server %s"
                                  (Conf.serverid_to_string serverid);
    Hashtbl.remove servers serverid

  let use ?usage_attempts f =
      (* We use retry here, since elements cannot be removed from an
         [Resource_pool.t] directly. Therefore we detect whether a server has been
         removed by our own means and try again with another server it this was
         the case. Once a server has been removed (by the use of [remove]) there
         will be [n] such retries before all traces of a server have been
         erased, where [n] equals the value used for [num_conn] when the server
         was added. *)
      Resource_pool.use ~usage_attempts:9 server_pool @@ fun {serverid; connections} ->
        begin
          if server_exists serverid then Lwt.return_unit else begin
            Lwt_log.ign_info_f ~section "cannot use server %s (removed)"
                                        (Conf.serverid_to_string serverid);
            Lwt.fail Resource_pool.(Resource_invalid {safe = true})
          end
        end >>= fun () ->
        Lwt_log.ign_debug_f ~section "using connection to server %s"
                                     (Conf.serverid_to_string serverid);
        Lwt.catch
          (fun () -> Resource_pool.use ?usage_attempts connections f)
          (fun e -> match e with
             | Resource_pool.(Resource_invalid {safe = true}) ->
                 Lwt_log.ign_warning
                   "connection unusable (safe to retry using another server)";
                 Lwt.fail e
             | Resource_pool.(Resource_invalid {safe = false}) ->
                 Lwt_log.ign_warning
                   "connection unusable (unsafe to retry using another server)";
                 Lwt.fail e
             | e -> Lwt.fail e
          )

  let servers () = Hashtbl.fold (fun server _ l -> server :: l) servers []

end
