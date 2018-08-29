(**
This module is built around [Resource_pool]. While a pool of type
[Resource_pool.t] manages a number of resources, here we manage a cluster of
such pools. A typical use case would be a cluster of servers, where for each
server we maintain a number of connections. A user of this module can call [use]
to access one of the connections, which are served in a round-robin fashion.
*)

module type CONF = sig
  type connection
  type server
  type serverid
  val serverid_to_string : serverid -> string (* for debugging purposes *)
  val connect : server -> connection Lwt.t
  val close : connection -> unit Lwt.t
end

module Make (Conf : CONF) : sig
  val servers : unit -> Conf.serverid list
  val server_exists : Conf.serverid -> bool
  (** [remove] marks a given server as removed from the pool. HOWEVER, a number
      of attempts (corresponding to the number of connections to that server)
      to use that server might still occur. These are NOT connection attempts,
      so this does not come with substantial costs. *)
  val remove : Conf.serverid -> unit

  (** Adds a server to the pool, permitting a maximum number [num_conn] of
      concurrent connections to that server. If [connect_immediately] is [true]
      (default: [false] then [num_conn] are immediately opened to the server.
      If the given server exists already no action is taken. *)
  val add_one :
    ?connect_immediately:bool ->
    num_conn:int -> Conf.serverid -> Conf.server -> unit
  (* Corresponds to multiple sequential applications of [add_one] except for the
     fact that [add_many] will result in a better initial scheduling
     distribution. *)
  val add_many :
    ?connect_immediately:bool ->
    num_conn:int -> (Conf.serverid * Conf.server) list -> unit
  (* Add a server for which a connection pool already exists *)
  val add_existing :
    num_conn:int -> Conf.serverid -> Conf.connection Resource_pool.t -> unit

  (* If [Resource_invalid] is raised by the supplied function, the connection is
     disposed of (using [close]), and a new attempt to run the function is
     launched to the same server as many times as specified by [usage_attempts]
     (default: 1). Be mindful of side-effects that the the function might have
     caused befaure raising [Resource_invalid]. *)
  val use : ?usage_attempts:int -> (Conf.connection -> 'a Lwt.t) -> 'a Lwt.t
end
