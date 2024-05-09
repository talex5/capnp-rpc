open Eio.Std

module Write = Eio.Buf_write

let src = Logs.Src.create "endpoint" ~doc:"Send and receive Cap'n'Proto messages"
module Log = (val Logs.src_log src: Logs.LOG)

let compression = `None

let _record_sent_messages = false

type flow = Eio.Flow.two_way_ty r

type t = {
  flow : flow;
  writer : Write.t;
  decoder : Capnp.Codecs.FramedStream.t;
  peer_id : Auth.Digest.t;
}

let peer_id t = t.peer_id

let of_flow ~peer_id flow =
  let decoder = Capnp.Codecs.FramedStream.empty compression in
  let flow = (flow :> flow) in
  let writer = Write.create 4096 in
  { flow; writer; decoder; peer_id }

let _dump_msg =
  let next = ref 0 in
  fun data ->
    let name = Fmt.str "/tmp/msg-%d.capnp" !next in
    Log.info (fun f -> f "Saved message as %S" name);
    incr next;
    let ch = open_out_bin name in
    output_string ch data;
    close_out ch

let send t msg =
  Capnp.Codecs.serialize_iter_copyless ~compression msg ~f:(fun x len -> Write.string t.writer x ~len)

let rec run t =
  (*   if record_sent_messages then dump_msg data; *)
  let bufs = Write.await_batch t.writer in
  match Eio.Flow.single_write t.flow bufs with
  | n -> Write.shift t.writer n; run t
  | exception End_of_file -> Error `Closed
  | exception (Eio.Io (Eio.Net.E Connection_reset _, _) as ex) ->
    Log.info (fun f -> f "%a" Eio.Exn.pp ex);
    Error `Closed
  | exception ex ->
    Eio.Fiber.check ();
    Error (`Msg (Printexc.to_string ex))

let rec recv t =
  match Capnp.Codecs.FramedStream.get_next_frame t.decoder with
  | Ok msg ->
    Write.pause t.writer;
    Ok (Capnp.BytesMessage.Message.readonly msg)
  | Error Capnp.Codecs.FramingError.Unsupported -> failwith "Unsupported Cap'n'Proto frame received"
  | Error Capnp.Codecs.FramingError.Incomplete ->
    Log.debug (fun f -> f "Incomplete; waiting for more data...");
    Fiber.yield ();
    Write.unpause t.writer;
    Fiber.yield ();
    let buf = Cstruct.create 4096 in    (* TODO: make this efficient *)
    match Eio.Flow.single_read t.flow buf with
    | got ->
      Log.debug (fun f -> f "Read %d bytes" got);
      Capnp.Codecs.FramedStream.add_fragment t.decoder (Cstruct.to_string buf ~len:got);
      recv t
    | exception End_of_file ->
      Error `Closed
    | exception (Eio.Io (Eio.Net.E Connection_reset _, _) as ex) ->
      Log.info (fun f -> f "%a" Eio.Exn.pp ex);
      Error `Closed

let disconnect t =
  try
    Eio.Flow.shutdown t.flow `All
  with Eio.Io (Eio.Net.E Connection_reset _, _) ->
    (* TCP connection already shut down, so TLS shutdown failed. Ignore. *)
    ()
