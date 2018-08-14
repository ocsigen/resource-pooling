[@@@ocaml.warning "+A-44-48"]

module Sequence = struct

  (* Inlining a small subset of BatDllist to prevent a heavy
     dependency on the whole batteries package *)

  exception Empty

  type 'a dllist = {
    mutable data : 'a;
    mutable next : 'a dllist;
    mutable prev : 'a dllist
  }

  let remove_dllist node =
    let next = node.next in
    if next == node then raise Empty;
    (* singleton list points to itself for next *)
    let prev = node.prev in
    (* Remove node from list by linking prev and next together *)
    prev.next <- next;
    next.prev <- prev;
    (* Make node a singleton list by setting its next and prev to itself *)
    node.next <- node;
    node.prev <- node

  let create_dllist x = let rec nn = { data = x; next = nn; prev = nn } in nn

  let prepend_dllist node elem =
    let nn = { data = elem; next = node; prev = node.prev } in
    node.prev.next <- nn;
    node.prev <- nn;
    nn

  let length_dllist node =
    let rec loop cnt n =
      if n == node then
        cnt
      else
        loop (cnt + 1) n.next
    in
    loop 1 node.next

  type 'a t = 'a dllist option

  let create () = None

  let take_opt_l l =
    match l with
    | Some l ->
      let res = l.data in
      remove_dllist l;
      Some res
    | None ->
      None

  let add_r e = function
    | None -> Some (create_dllist e)
    | Some l ->
      let _ = prepend_dllist l e in
      Some l

  let remove = function
    | None -> ()
    | Some l -> remove_dllist l

  let length = function
    | None -> 0
    | Some l -> length_dllist l

end

open Lwt.Infix

type 'a t = {
  create : unit -> 'a Lwt.t;
  (* Create a new pool member. *)
  check : 'a -> (bool -> unit) -> unit;
  (* Check validity of a pool member when use resulted in failed promise. *)
  validate : 'a -> bool Lwt.t;
  (* Validate an existing free pool member before use. *)
  dispose : 'a -> unit Lwt.t;
  (* Dispose of a pool member. *)
  cleared : bool ref ref;
  (* Have the current pool elements been cleared out? *)
  mutable max : int;
  (* Size of the pool. *)
  mutable count : int;
  (* Number of elements in the pool. *)
  list : 'a Queue.t;
  (* Available pool members. *)
  waiters : 'a Lwt.u Sequence.t;
  (* Promise resolvers waiting for a free member. *)
}

let create
  ?(validate = fun _ -> Lwt.return_true)
  ?(check = fun _ f -> f true)
  ?(dispose = fun _ -> Lwt.return_unit)
  max
  create = {
    max; create; validate; check; dispose;
    cleared = ref (ref false);
    count = 0;
    list = Queue.create ();
    waiters = Sequence.create ()
  }

let set_max p n = p.max <- n

(* Create a pool member. *)
let create_member p =
  Lwt.catch
    (fun () ->
       (* Must be done before p.create to prevent other resolvers from
          creating new members if the limit is reached. *)
       p.count <- p.count + 1;
       p.create ())
    (fun exn ->
       (* Creation failed, so don't increment count. *)
       p.count <- p.count - 1;
       Lwt.fail exn)

(* Release a pool member. *)
let release p c =
  match Sequence.take_opt_l p.waiters with
  | Some wakener ->
    (* A promise resolver is waiting, give it the pool member. *)
    Lwt.wakeup_later wakener c
  | None ->
    (* No one is waiting, queue it. *)
    Queue.push c p.list

exception Resource_limit_exceeded

let add ?(omit_max_check = false) p c =
  if not omit_max_check && p.count >= p.max then raise Resource_limit_exceeded;
  p.count <- p.count + 1;
  release p c

(* Dispose of a pool member. *)
let dispose p c =
  p.dispose c >>= fun () ->
  p.count <- p.count - 1;
  Lwt.return_unit

(* Create a new member when one is thrown away. *)
let replace_disposed p =
  match Sequence.take_opt_l p.waiters with
  | None ->
    (* No one is waiting, do not create a new member to avoid
       losing an error if creation fails. *)
    ()
  | Some wakener ->
    Lwt.on_any
      (Lwt.apply p.create ())
      (fun c ->
         Lwt.wakeup_later wakener c)
      (fun exn ->
         (* Creation failed, notify the waiter of the failure. *)
         Lwt.wakeup_later_exn wakener exn)

(* Verify a member is still valid before using it. *)
let validate_and_return p c =
  Lwt.try_bind
      (fun () ->
         p.validate c)
      (function
        | true ->
          Lwt.return c
        | false ->
          (* Remove this member and create a new one. *)
          dispose p c >>= fun () ->
          create_member p)
      (fun e ->
         (* Validation failed: create a new member if at least one
            resolver is waiting. *)
         dispose p c >>= fun () ->
         replace_disposed p;
         Lwt.fail e)

exception Resource_invalid

let add_task_r sequence =
  let p, r = Lwt.task () in
  let node = Sequence.add_r r sequence in
  Lwt.on_cancel p (fun () -> Sequence.remove node);
  p

(* Acquire a pool member. *)
let acquire ~attempts p =
  assert (attempts > 0);
  let once () =
    if Queue.is_empty p.list then
      (* No more available member. *)
      if p.count < p.max then
        (* Limit not reached: create a new one. *)
        create_member p
      else
        (* Limit reached: wait for a free one. *)
        add_task_r p.waiters >>= validate_and_return p
    else
      (* Take the first free member and validate it. *)
      let c = Queue.take p.list in
      validate_and_return p c
  in
  let rec keep_trying attempts =
    if attempts > 0
      then Lwt.catch once @@ fun e ->
        if e = Resource_invalid then keep_trying (attempts - 1) else Lwt.fail e
      else Lwt.fail Resource_invalid
  in keep_trying attempts

(* Release a member when use resulted in failed promise if the member
   is still valid. *)
let check_and_release p c cleared =
  let ok = ref false in
  p.check c (fun result -> ok := result);
  if cleared || not !ok then (
    (* Element is not ok or the pool was cleared - dispose of it *)
    dispose p c
  )
  else (
    (* Element is ok - release it back to the pool *)
    release p c;
    Lwt.return_unit
  )

let use ?(creation_attempts = 1) ?(usage_attempts = 1) p f =
  assert (usage_attempts > 0);
  let cleared = !(p.cleared) in
  (* Capture the current cleared state so we can see if it changes while this
     element is in use *)
  let rec make_promise attempts =
    if attempts <= 0 then Lwt.fail Resource_invalid else
    acquire ~attempts:creation_attempts p >>= fun c ->
    Lwt.catch
      (fun () -> f c >>= fun res -> Lwt.return (c,res))
      (function
         | Resource_invalid ->
             dispose p c >>= fun () ->
             make_promise (attempts - 1)
         | e ->
             check_and_release p c !cleared >>= fun () ->
             Lwt.fail e)
  in
  let promise = make_promise usage_attempts in
  promise >>= fun (c,_) ->
  if !cleared then (
    (* p was cleared while promise was resolving - dispose of this element *)
    dispose p c >>= fun () ->
    Lwt.map snd promise
  )
  else (
    release p c;
    Lwt.map snd promise
  )

let clear p =
  let elements = Queue.fold (fun l element -> element :: l) [] p.list in
  Queue.clear p.list;
  (* Indicate to any currently in-use elements that we cleared the pool *)
  let old_cleared = !(p.cleared) in
  old_cleared := true;
  p.cleared := ref false;
  Lwt_list.iter_s (dispose p) elements

let wait_queue_length p = Sequence.length p.waiters
