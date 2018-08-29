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


(* As we use nested pools, we need to make sure that the exception
   [Lwt.Resource_invalid affects] the correct pool. If it is raised by the
   function [f] supplied to [use] it should affect the inner pool. If it is
   raised by the function [use] of this module it should affect the outer
   pool. Therefore we transform a [Lwt.Resource_invalid] raised by [f] into a
   [Inner_resource_invalid] to distinguish it from the outer exception. At
   the end of [use] we transform it back to [Lwt.Resource_invalid] which is
   what the user of [use] would expect. The outer [Resource_invalid] is used
   to signal that a server has been removed from the pool, in which case we
   retry using another server. Note that at this point [f] has not yet been
   called, so there is no problem concerning side-effects. *)
exception Inner_resource_invalid

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

  (* Each server holds its own connection_pool, so a server pool is a pool of
     connection pools. HOWEVER, [server_pool] will not contain one
     connection pool per server, but [n] connection pools per server, where [n]
     is the (maximum) size of the servers connection pool. *)
  let server_pool : connection_pool Resource_pool.t =
    let nil () = failwith "Bs_db.server_pool: invalid connection attempt" in
    (* We supply [0] as the first argument to [Resource_pool.create] as it will
       prevent [Resource_pool] to ever create a new resource on its own. This is what
       we want since new servers are to be added by the user of this module. *)
    Resource_pool.create 0 nil

  let servers : (Conf.serverid, unit) Hashtbl.t = Hashtbl.create 9

  let server_exists serverid = Hashtbl.mem servers serverid

  let add_many ?(connect_immediately = false) ~num_conn new_servers =
    let mk_connection_pool (serverid, server) : connection_pool =
      Lwt_log.ign_notice_f ~section "adding server: %s"
        (Conf.serverid_to_string serverid);
      let connect () =
        Lwt_log.ign_info ~section @@
        Printf.sprintf "opening connection to server %s"
          (Conf.serverid_to_string serverid);
        Conf.connect server
      in
      let dispose conn =
        Lwt_log.ign_info ~section @@
        Printf.sprintf "closing connection to server %s"
          (Conf.serverid_to_string serverid);
        Lwt.catch (fun () -> Conf.close conn) (fun _ -> Lwt.return_unit)
      in
      let conn_pool = Resource_pool.create num_conn ~dispose connect in
      if connect_immediately then
        for _ = 1 to num_conn do
          Lwt.async @@ fun () ->
            connect () >>= fun c ->
            try Resource_pool.add conn_pool c; Lwt.return_unit
            with Resource_pool.Resource_limit_exceeded -> dispose c
        done;
      Hashtbl.add servers serverid ();
      {serverid; connections = conn_pool}
    in
    let pools = List.map mk_connection_pool @@
      List.filter (fun l -> not (server_exists (fst l))) new_servers in
    for _ = 1 to num_conn do
      List.iter (Resource_pool.add ~omit_max_check:true server_pool) pools
    done

  let add_one ?connect_immediately ~num_conn serverid server =
    add_many ?connect_immediately ~num_conn [(serverid, server)]

  let add_existing ~num_conn serverid connections =
    Lwt_log.ign_notice_f ~section "adding existing server: %s"
      (Conf.serverid_to_string serverid);
    Hashtbl.add servers serverid ();
    for _ = 1 to num_conn do
      Resource_pool.add ~omit_max_check:true server_pool {serverid; connections}
    done;
    ()

  let remove serverid =
    Lwt_log.ign_notice_f ~section "removing server %s"
      (Conf.serverid_to_string serverid);
    Hashtbl.remove servers serverid

  let use ?usage_attempts f =

    let wrap_resource_invalid f = Lwt.catch f @@ function
      | Resource_pool.Resource_invalid -> Lwt.fail Inner_resource_invalid
      | e -> Lwt.fail e
    in
    let unwrap_resource_invalid f = Lwt.catch f @@ function
      | Inner_resource_invalid -> Lwt.fail Resource_pool.Resource_invalid
      | e -> Lwt.fail e
    in

    unwrap_resource_invalid @@ fun () ->
      (* We use retry here, since elements cannot be removed from an
         [Resource_pool.t] directly. Therefore we detect whether a server has been
         removed by our own means and try again with another server it this was
         the case. Once a server has been removed (by the use of [remove]) there
         will be [n] such retries before all traces of a server have been
         erased, where [n] equals the value used for [num_conn] when the server
         was added. *)
      Resource_pool.use ~usage_attempts:9 server_pool @@ fun {serverid; connections} ->
        begin
          if not @@ server_exists serverid
            then begin
              Lwt_log.ign_info ~section @@
              Printf.sprintf "cannot use server %s (removed)"
                (Conf.serverid_to_string serverid);
              Lwt.fail Resource_pool.Resource_invalid
            end
            else Lwt.return_unit
        end >>= fun () ->
        Lwt_log.ign_debug ~section @@
        Printf.sprintf "using connection to server %s"
          (Conf.serverid_to_string serverid);
        wrap_resource_invalid (fun () -> Resource_pool.use ?usage_attempts connections f)

  let servers () = Hashtbl.fold (fun server _ l -> server :: l) servers []

end
