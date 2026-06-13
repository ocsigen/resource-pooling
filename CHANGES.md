## 1.3

* Replace the deprecated `lwt_log` with `logs`/`logs.lwt` for logging
  (`Server_pool.section` is now a `Logs.src` instead of `Lwt_log.section`).
  This removes the `lwt_log` dependency, which capped `lwt < 6.0.0`, and thus
  unblocks builds against Lwt >= 6.
* Depend explicitly on `lwt.unix` (previously pulled transitively via
  `lwt_log`).
