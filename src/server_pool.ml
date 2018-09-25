(**
This module is built around [Resource_pool]. While a pool of type
[Resource_pool.t] manages a number of resources, here we manage a cluster of
such pools. A typical use case would be a cluster of servers, where for each
server we maintain a number of connections. A user of this module can call [use]
to access one of the connections, which are served in a round-robin fashion.
*)

[@@@ocaml.warning "+A-9-44-48"]

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
  val check_delay : float
  val check_server : serverid -> server -> bool Lwt.t
end

module Make (Conf : CONF) = struct

  let show = Conf.serverid_to_string

  type connection_pool = {
    serverid : Conf.serverid;
    connections : Conf.connection Resource_pool.t
  }

  type server_status = {
    desired : int;
    current : int;
    essential : bool;
    suspended : bool
  }

  let server_status ~desired ~essential =
    {desired; current = 0; essential; suspended = false}

  let servers : (Conf.serverid, server_status) Hashtbl.t = Hashtbl.create 9

  let remove serverid =
    Lwt_log.ign_notice_f ~section "removing server %s" (show serverid);
    Hashtbl.remove servers serverid

  let update_current_count serverid f =
    let status =
      let old_status = Hashtbl.find servers serverid in
      {old_status with current = f old_status.current}
    in
    Hashtbl.replace servers serverid status;
    Lwt_log.ign_info_f ~section "current number of instances for %s: %d/%d"
                                (show serverid) status.current status.desired

  (* Each server holds its own connection_pool, so a server pool is a pool of
     connection pools. HOWEVER, [server_pool] will not contain one
     connection pool per server, but [n] times the same connection pool per
     server, where [n] is the (maximum) size of the servers connection pool. *)
  let server_pool : connection_pool Resource_pool.t =
    let nil () = failwith "Bs_db.server_pool: invalid connection attempt" in
    (* We supply [0] as the first argument to [Resource_pool.create] as it will
       prevent [Resource_pool] to ever create a new resource on its own. This is what
       we want since new servers are to be added by the user of this module. *)
    let n = 0
    and dispose {serverid} =
      update_current_count serverid pred;
      Lwt.return_unit
    and check {serverid} f = (* retire non-essential servers *)
      let status = Hashtbl.find servers serverid in
      f status.essential
    in Resource_pool.create ~check ~dispose n nil

  let server_exists serverid = Hashtbl.mem servers serverid

  let add_many
      ?(essential = false)
      ?(connect_immediately = false) ~num_conn new_servers =
    let mk_connection_pool (serverid, server) : connection_pool =
      Lwt_log.ign_notice_f ~section "adding server: %s" (show serverid);
      let connect () =
        Lwt_log.ign_info_f ~section "opening connection to %s" (show serverid);
        Conf.connect server >>= fun conn ->
        Lwt.return conn
      in
      let close_connections_r = ref Lwt.return in
      let reactivate_server_r = ref Lwt.return in
      let suspend_server () =
        let server = Hashtbl.find servers serverid in
        if server.essential || server.suspended then () else begin
          Lwt_log.ign_notice_f ~section "suspending %s" (show serverid);
          Hashtbl.replace servers serverid {server with suspended = true};
          Lwt.async @@ fun () ->
            !close_connections_r () >>= fun () ->
            !reactivate_server_r ()
        end
      in
      let dispose conn =
        Lwt_log.ign_info_f ~section "closing connection to %s" (show serverid);
        Lwt.catch (fun () -> Conf.close conn) (fun _ -> Lwt.return_unit) >>= fun () ->
        suspend_server ();
        Lwt.return_unit
      in
      let status = server_status ~desired:num_conn ~essential in
      Hashtbl.add servers serverid status;
      let check _ f = f false in (* always close connections *)
      let conn_pool = Resource_pool.create num_conn ~check ~dispose connect in
      let pool = {serverid; connections = conn_pool} in
      let close_connections () = Resource_pool.clear conn_pool in
      close_connections_r := close_connections;
      let rec reactivate_server () =
        Lwt_unix.sleep Conf.check_delay >>= fun () ->
        Lwt_log.ign_debug_f ~section "checking server health of %s" (show serverid);
        Lwt.catch
          (fun () -> Conf.check_server serverid server)
          (fun e ->
             Lwt_log.ign_info_f ~section
               "exception during health check of %s: %s"
               (show serverid) (Printexc.to_string e);
             Lwt.return_false)
        >>= fun healthy ->
        if healthy
          then begin
            Lwt_log.ign_info_f ~section
              "reactivating healthy server %s" (show serverid);
            let status = Hashtbl.find servers serverid in
            Hashtbl.replace servers serverid {status with suspended = false};
            for _ = status.current to status.desired - 1 do
              Resource_pool.add ~omit_max_check:true server_pool pool;
              update_current_count serverid succ
            done;
            Lwt.return_unit
          end
          else reactivate_server ()
      in
      reactivate_server_r := reactivate_server;
      if connect_immediately then
        for _ = 1 to num_conn do
          Lwt.async @@ fun () ->
            connect () >>= fun c ->
            try Resource_pool.add conn_pool c; Lwt.return_unit
            with Resource_pool.Resource_limit_exceeded -> dispose c
        done;
      pool
    in
    let pools = List.map mk_connection_pool @@
      List.filter (fun l -> not @@ server_exists @@ fst l) new_servers in
    for _ = 1 to num_conn do
      pools |> List.iter @@ fun conn_pool ->
        Resource_pool.add ~omit_max_check:true server_pool conn_pool;
        update_current_count conn_pool.serverid succ
    done

  let add_one ?essential ?connect_immediately ~num_conn serverid server =
    add_many ?essential ?connect_immediately ~num_conn [(serverid, server)]

  let add_existing ?(essential = false) ~num_conn serverid connections =
    Lwt_log.ign_notice_f ~section "adding existing server: %s" (show serverid);
    let status = server_status ~desired:num_conn ~essential in
    Hashtbl.add servers serverid status;
    for _ = 1 to num_conn do
      Resource_pool.add ~omit_max_check:true server_pool {serverid; connections};
      update_current_count serverid succ
    done

  let use ?usage_attempts f =
    (* We use retry here, since elements cannot be removed from an
       [Resource_pool.t] directly. Therefore we detect whether a server has been
       removed by our own means and try again with another server it this was
       the case. Once a server has been removed (by the use of [remove]) there
       will be [n] such retries before all traces of a server have been
       erased, where [n] equals the value used for [num_conn] when the server
       was added. *)
    Resource_pool.use ~usage_attempts:9 server_pool @@ fun {serverid; connections} ->
      let server_o = try Some (Hashtbl.find servers serverid)
                     with Not_found -> None
      in
      match server_o with
      | None ->
          Lwt_log.ign_info_f ~section "cannot use %s (removed)" (show serverid);
          Lwt.fail Resource_pool.(Resource_invalid {safe = true})
      | Some {suspended = true} ->
          Lwt_log.ign_info_f ~section "not using %s (suspended)" (show serverid);
          Lwt.fail Resource_pool.(Resource_invalid {safe = true})
      | Some _ ->
        Lwt_log.ign_debug_f ~section "using connection to %s" (show serverid);
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
