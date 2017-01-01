(function() {var implementors = {};
implementors["futures"] = [];implementors["relay"] = ["impl&lt;T&gt; <a class='trait' href='futures/future/trait.Future.html' title='futures::future::Future'>Future</a> for <a class='struct' href='relay/struct.Receiver.html' title='relay::Receiver'>Receiver</a>&lt;T&gt;","impl&lt;T&gt; <a class='trait' href='futures/future/trait.Future.html' title='futures::future::Future'>Future</a> for <a class='struct' href='relay/struct.Waiting.html' title='relay::Waiting'>Waiting</a>&lt;T&gt;",];implementors["futures_cpupool"] = ["impl&lt;T:&nbsp;<a class='trait' href='https://doc.rust-lang.org/nightly/core/marker/trait.Send.html' title='core::marker::Send'>Send</a> + 'static, E:&nbsp;<a class='trait' href='https://doc.rust-lang.org/nightly/core/marker/trait.Send.html' title='core::marker::Send'>Send</a> + 'static&gt; <a class='trait' href='futures/future/trait.Future.html' title='futures::future::Future'>Future</a> for <a class='struct' href='futures_cpupool/struct.CpuFuture.html' title='futures_cpupool::CpuFuture'>CpuFuture</a>&lt;T, E&gt;",];implementors["tokio_core"] = ["impl&lt;R, W&gt; <a class='trait' href='futures/future/trait.Future.html' title='futures::future::Future'>Future</a> for <a class='struct' href='tokio_core/io/struct.Copy.html' title='tokio_core::io::Copy'>Copy</a>&lt;R, W&gt; <span class='where'>where R: <a class='trait' href='https://doc.rust-lang.org/nightly/std/io/trait.Read.html' title='std::io::Read'>Read</a>, W: <a class='trait' href='https://doc.rust-lang.org/nightly/std/io/trait.Write.html' title='std::io::Write'>Write</a></span>","impl&lt;A&gt; <a class='trait' href='futures/future/trait.Future.html' title='futures::future::Future'>Future</a> for <a class='struct' href='tokio_core/io/struct.Flush.html' title='tokio_core::io::Flush'>Flush</a>&lt;A&gt; <span class='where'>where A: <a class='trait' href='https://doc.rust-lang.org/nightly/std/io/trait.Write.html' title='std::io::Write'>Write</a></span>","impl&lt;A, T&gt; <a class='trait' href='futures/future/trait.Future.html' title='futures::future::Future'>Future</a> for <a class='struct' href='tokio_core/io/struct.ReadExact.html' title='tokio_core::io::ReadExact'>ReadExact</a>&lt;A, T&gt; <span class='where'>where A: <a class='trait' href='https://doc.rust-lang.org/nightly/std/io/trait.Read.html' title='std::io::Read'>Read</a>, T: <a class='trait' href='https://doc.rust-lang.org/nightly/core/convert/trait.AsMut.html' title='core::convert::AsMut'>AsMut</a>&lt;<a class='primitive' href='https://doc.rust-lang.org/nightly/std/primitive.slice.html'>[</a><a class='primitive' href='https://doc.rust-lang.org/nightly/std/primitive.u8.html'>u8</a><a class='primitive' href='https://doc.rust-lang.org/nightly/std/primitive.slice.html'>]</a>&gt;</span>","impl&lt;A&gt; <a class='trait' href='futures/future/trait.Future.html' title='futures::future::Future'>Future</a> for <a class='struct' href='tokio_core/io/struct.ReadToEnd.html' title='tokio_core::io::ReadToEnd'>ReadToEnd</a>&lt;A&gt; <span class='where'>where A: <a class='trait' href='https://doc.rust-lang.org/nightly/std/io/trait.Read.html' title='std::io::Read'>Read</a></span>","impl&lt;R, T&gt; <a class='trait' href='futures/future/trait.Future.html' title='futures::future::Future'>Future</a> for <a class='struct' href='tokio_core/io/struct.Read.html' title='tokio_core::io::Read'>Read</a>&lt;R, T&gt; <span class='where'>where R: <a class='trait' href='https://doc.rust-lang.org/nightly/std/io/trait.Read.html' title='std::io::Read'>Read</a>, T: <a class='trait' href='https://doc.rust-lang.org/nightly/core/convert/trait.AsMut.html' title='core::convert::AsMut'>AsMut</a>&lt;<a class='primitive' href='https://doc.rust-lang.org/nightly/std/primitive.slice.html'>[</a><a class='primitive' href='https://doc.rust-lang.org/nightly/std/primitive.u8.html'>u8</a><a class='primitive' href='https://doc.rust-lang.org/nightly/std/primitive.slice.html'>]</a>&gt;</span>","impl&lt;A&gt; <a class='trait' href='futures/future/trait.Future.html' title='futures::future::Future'>Future</a> for <a class='struct' href='tokio_core/io/struct.ReadUntil.html' title='tokio_core::io::ReadUntil'>ReadUntil</a>&lt;A&gt; <span class='where'>where A: <a class='trait' href='https://doc.rust-lang.org/nightly/std/io/trait.Read.html' title='std::io::Read'>Read</a> + <a class='trait' href='https://doc.rust-lang.org/nightly/std/io/trait.BufRead.html' title='std::io::BufRead'>BufRead</a></span>","impl&lt;A, T&gt; <a class='trait' href='futures/future/trait.Future.html' title='futures::future::Future'>Future</a> for <a class='struct' href='tokio_core/io/struct.WriteAll.html' title='tokio_core::io::WriteAll'>WriteAll</a>&lt;A, T&gt; <span class='where'>where A: <a class='trait' href='https://doc.rust-lang.org/nightly/std/io/trait.Write.html' title='std::io::Write'>Write</a>, T: <a class='trait' href='https://doc.rust-lang.org/nightly/core/convert/trait.AsRef.html' title='core::convert::AsRef'>AsRef</a>&lt;<a class='primitive' href='https://doc.rust-lang.org/nightly/std/primitive.slice.html'>[</a><a class='primitive' href='https://doc.rust-lang.org/nightly/std/primitive.u8.html'>u8</a><a class='primitive' href='https://doc.rust-lang.org/nightly/std/primitive.slice.html'>]</a>&gt;</span>","impl <a class='trait' href='futures/future/trait.Future.html' title='futures::future::Future'>Future</a> for <a class='struct' href='tokio_core/net/struct.TcpStreamNew.html' title='tokio_core::net::TcpStreamNew'>TcpStreamNew</a>","impl&lt;T&gt; <a class='trait' href='futures/future/trait.Future.html' title='futures::future::Future'>Future</a> for <a class='struct' href='tokio_core/net/struct.SendDgram.html' title='tokio_core::net::SendDgram'>SendDgram</a>&lt;T&gt; <span class='where'>where T: <a class='trait' href='https://doc.rust-lang.org/nightly/core/convert/trait.AsRef.html' title='core::convert::AsRef'>AsRef</a>&lt;<a class='primitive' href='https://doc.rust-lang.org/nightly/std/primitive.slice.html'>[</a><a class='primitive' href='https://doc.rust-lang.org/nightly/std/primitive.u8.html'>u8</a><a class='primitive' href='https://doc.rust-lang.org/nightly/std/primitive.slice.html'>]</a>&gt;</span>","impl&lt;T&gt; <a class='trait' href='futures/future/trait.Future.html' title='futures::future::Future'>Future</a> for <a class='struct' href='tokio_core/net/struct.RecvDgram.html' title='tokio_core::net::RecvDgram'>RecvDgram</a>&lt;T&gt; <span class='where'>where T: <a class='trait' href='https://doc.rust-lang.org/nightly/core/convert/trait.AsMut.html' title='core::convert::AsMut'>AsMut</a>&lt;<a class='primitive' href='https://doc.rust-lang.org/nightly/std/primitive.slice.html'>[</a><a class='primitive' href='https://doc.rust-lang.org/nightly/std/primitive.u8.html'>u8</a><a class='primitive' href='https://doc.rust-lang.org/nightly/std/primitive.slice.html'>]</a>&gt;</span>","impl <a class='trait' href='futures/future/trait.Future.html' title='futures::future::Future'>Future</a> for <a class='struct' href='tokio_core/reactor/struct.Timeout.html' title='tokio_core::reactor::Timeout'>Timeout</a>",];implementors["tokio_tls"] = ["impl&lt;S:&nbsp;<a class='trait' href='tokio_core/io/trait.Io.html' title='tokio_core::io::Io'>Io</a>&gt; <a class='trait' href='futures/future/trait.Future.html' title='futures::future::Future'>Future</a> for <a class='struct' href='tokio_tls/struct.ConnectAsync.html' title='tokio_tls::ConnectAsync'>ConnectAsync</a>&lt;S&gt;","impl&lt;S:&nbsp;<a class='trait' href='tokio_core/io/trait.Io.html' title='tokio_core::io::Io'>Io</a>&gt; <a class='trait' href='futures/future/trait.Future.html' title='futures::future::Future'>Future</a> for <a class='struct' href='tokio_tls/struct.AcceptAsync.html' title='tokio_tls::AcceptAsync'>AcceptAsync</a>&lt;S&gt;",];implementors["tokio_proto"] = ["impl&lt;T&gt; <a class='trait' href='futures/future/trait.Future.html' title='futures::future::Future'>Future</a> for <a class='struct' href='tokio_proto/streaming/pipeline/advanced/struct.Pipeline.html' title='tokio_proto::streaming::pipeline::advanced::Pipeline'>Pipeline</a>&lt;T&gt; <span class='where'>where T: <a class='trait' href='tokio_proto/streaming/pipeline/advanced/trait.Dispatch.html' title='tokio_proto::streaming::pipeline::advanced::Dispatch'>Dispatch</a></span>","impl&lt;T&gt; <a class='trait' href='futures/future/trait.Future.html' title='futures::future::Future'>Future</a> for <a class='struct' href='tokio_proto/streaming/multiplex/advanced/struct.Multiplex.html' title='tokio_proto::streaming::multiplex::advanced::Multiplex'>Multiplex</a>&lt;T&gt; <span class='where'>where T: <a class='trait' href='tokio_proto/streaming/multiplex/advanced/trait.Dispatch.html' title='tokio_proto::streaming::multiplex::advanced::Dispatch'>Dispatch</a></span>","impl&lt;T, E&gt; <a class='trait' href='futures/future/trait.Future.html' title='futures::future::Future'>Future</a> for <a class='struct' href='tokio_proto/util/client_proxy/struct.Response.html' title='tokio_proto::util::client_proxy::Response'>Response</a>&lt;T, E&gt; <span class='where'>where E: <a class='trait' href='https://doc.rust-lang.org/nightly/core/convert/trait.From.html' title='core::convert::From'>From</a>&lt;<a class='struct' href='https://doc.rust-lang.org/nightly/std/io/error/struct.Error.html' title='std::io::error::Error'>Error</a>&gt;</span>","impl&lt;Kind, P&gt; <a class='trait' href='futures/future/trait.Future.html' title='futures::future::Future'>Future</a> for <a class='struct' href='tokio_proto/struct.Connect.html' title='tokio_proto::Connect'>Connect</a>&lt;Kind, P&gt; <span class='where'>where P: <a class='trait' href='tokio_proto/trait.BindClient.html' title='tokio_proto::BindClient'>BindClient</a>&lt;Kind, <a class='struct' href='tokio_core/net/tcp/struct.TcpStream.html' title='tokio_core::net::tcp::TcpStream'>TcpStream</a>&gt;</span>",];implementors["hyper"] = ["impl&lt;S&gt; <a class='trait' href='futures/future/trait.Future.html' title='futures::future::Future'>Future</a> for <a class='struct' href='tokio_tls/struct.ConnectAsync.html' title='tokio_tls::ConnectAsync'>ConnectAsync</a>&lt;S&gt; <span class='where'>where S: <a class='trait' href='tokio_core/io/trait.Io.html' title='tokio_core::io::Io'>Io</a></span>","impl&lt;S&gt; <a class='trait' href='futures/future/trait.Future.html' title='futures::future::Future'>Future</a> for <a class='struct' href='tokio_tls/struct.AcceptAsync.html' title='tokio_tls::AcceptAsync'>AcceptAsync</a>&lt;S&gt; <span class='where'>where S: <a class='trait' href='tokio_core/io/trait.Io.html' title='tokio_core::io::Io'>Io</a></span>","impl&lt;T&gt; <a class='trait' href='futures/future/trait.Future.html' title='futures::future::Future'>Future</a> for <a class='struct' href='relay/struct.Receiver.html' title='relay::Receiver'>Receiver</a>&lt;T&gt;","impl&lt;T&gt; <a class='trait' href='futures/future/trait.Future.html' title='futures::future::Future'>Future</a> for <a class='struct' href='relay/struct.Waiting.html' title='relay::Waiting'>Waiting</a>&lt;T&gt;","impl&lt;T, E&gt; <a class='trait' href='futures/future/trait.Future.html' title='futures::future::Future'>Future</a> for <a class='struct' href='futures_cpupool/struct.CpuFuture.html' title='futures_cpupool::CpuFuture'>CpuFuture</a>&lt;T, E&gt; <span class='where'>where E: <a class='trait' href='https://doc.rust-lang.org/nightly/core/marker/trait.Send.html' title='core::marker::Send'>Send</a> + 'static, T: <a class='trait' href='https://doc.rust-lang.org/nightly/core/marker/trait.Send.html' title='core::marker::Send'>Send</a> + 'static</span>","impl&lt;R, W&gt; <a class='trait' href='futures/future/trait.Future.html' title='futures::future::Future'>Future</a> for <a class='struct' href='tokio_core/io/copy/struct.Copy.html' title='tokio_core::io::copy::Copy'>Copy</a>&lt;R, W&gt; <span class='where'>where R: <a class='trait' href='https://doc.rust-lang.org/nightly/std/io/trait.Read.html' title='std::io::Read'>Read</a>, W: <a class='trait' href='https://doc.rust-lang.org/nightly/std/io/trait.Write.html' title='std::io::Write'>Write</a></span>","impl&lt;A&gt; <a class='trait' href='futures/future/trait.Future.html' title='futures::future::Future'>Future</a> for <a class='struct' href='tokio_core/io/flush/struct.Flush.html' title='tokio_core::io::flush::Flush'>Flush</a>&lt;A&gt; <span class='where'>where A: <a class='trait' href='https://doc.rust-lang.org/nightly/std/io/trait.Write.html' title='std::io::Write'>Write</a></span>","impl&lt;A, T&gt; <a class='trait' href='futures/future/trait.Future.html' title='futures::future::Future'>Future</a> for <a class='struct' href='tokio_core/io/read_exact/struct.ReadExact.html' title='tokio_core::io::read_exact::ReadExact'>ReadExact</a>&lt;A, T&gt; <span class='where'>where A: <a class='trait' href='https://doc.rust-lang.org/nightly/std/io/trait.Read.html' title='std::io::Read'>Read</a>, T: <a class='trait' href='https://doc.rust-lang.org/nightly/core/convert/trait.AsMut.html' title='core::convert::AsMut'>AsMut</a>&lt;<a class='primitive' href='https://doc.rust-lang.org/nightly/std/primitive.slice.html'>[</a><a class='primitive' href='https://doc.rust-lang.org/nightly/std/primitive.u8.html'>u8</a><a class='primitive' href='https://doc.rust-lang.org/nightly/std/primitive.slice.html'>]</a>&gt;</span>","impl&lt;A&gt; <a class='trait' href='futures/future/trait.Future.html' title='futures::future::Future'>Future</a> for <a class='struct' href='tokio_core/io/read_to_end/struct.ReadToEnd.html' title='tokio_core::io::read_to_end::ReadToEnd'>ReadToEnd</a>&lt;A&gt; <span class='where'>where A: <a class='trait' href='https://doc.rust-lang.org/nightly/std/io/trait.Read.html' title='std::io::Read'>Read</a></span>","impl&lt;R, T&gt; <a class='trait' href='futures/future/trait.Future.html' title='futures::future::Future'>Future</a> for <a class='struct' href='tokio_core/io/read/struct.Read.html' title='tokio_core::io::read::Read'>Read</a>&lt;R, T&gt; <span class='where'>where R: <a class='trait' href='https://doc.rust-lang.org/nightly/std/io/trait.Read.html' title='std::io::Read'>Read</a>, T: <a class='trait' href='https://doc.rust-lang.org/nightly/core/convert/trait.AsMut.html' title='core::convert::AsMut'>AsMut</a>&lt;<a class='primitive' href='https://doc.rust-lang.org/nightly/std/primitive.slice.html'>[</a><a class='primitive' href='https://doc.rust-lang.org/nightly/std/primitive.u8.html'>u8</a><a class='primitive' href='https://doc.rust-lang.org/nightly/std/primitive.slice.html'>]</a>&gt;</span>","impl&lt;A&gt; <a class='trait' href='futures/future/trait.Future.html' title='futures::future::Future'>Future</a> for <a class='struct' href='tokio_core/io/read_until/struct.ReadUntil.html' title='tokio_core::io::read_until::ReadUntil'>ReadUntil</a>&lt;A&gt; <span class='where'>where A: <a class='trait' href='https://doc.rust-lang.org/nightly/std/io/trait.Read.html' title='std::io::Read'>Read</a> + <a class='trait' href='https://doc.rust-lang.org/nightly/std/io/trait.BufRead.html' title='std::io::BufRead'>BufRead</a></span>","impl&lt;A, T&gt; <a class='trait' href='futures/future/trait.Future.html' title='futures::future::Future'>Future</a> for <a class='struct' href='tokio_core/io/write_all/struct.WriteAll.html' title='tokio_core::io::write_all::WriteAll'>WriteAll</a>&lt;A, T&gt; <span class='where'>where A: <a class='trait' href='https://doc.rust-lang.org/nightly/std/io/trait.Write.html' title='std::io::Write'>Write</a>, T: <a class='trait' href='https://doc.rust-lang.org/nightly/core/convert/trait.AsRef.html' title='core::convert::AsRef'>AsRef</a>&lt;<a class='primitive' href='https://doc.rust-lang.org/nightly/std/primitive.slice.html'>[</a><a class='primitive' href='https://doc.rust-lang.org/nightly/std/primitive.u8.html'>u8</a><a class='primitive' href='https://doc.rust-lang.org/nightly/std/primitive.slice.html'>]</a>&gt;</span>","impl <a class='trait' href='futures/future/trait.Future.html' title='futures::future::Future'>Future</a> for <a class='struct' href='tokio_core/net/tcp/struct.TcpStreamNew.html' title='tokio_core::net::tcp::TcpStreamNew'>TcpStreamNew</a>","impl&lt;T&gt; <a class='trait' href='futures/future/trait.Future.html' title='futures::future::Future'>Future</a> for <a class='struct' href='tokio_core/net/udp/struct.SendDgram.html' title='tokio_core::net::udp::SendDgram'>SendDgram</a>&lt;T&gt; <span class='where'>where T: <a class='trait' href='https://doc.rust-lang.org/nightly/core/convert/trait.AsRef.html' title='core::convert::AsRef'>AsRef</a>&lt;<a class='primitive' href='https://doc.rust-lang.org/nightly/std/primitive.slice.html'>[</a><a class='primitive' href='https://doc.rust-lang.org/nightly/std/primitive.u8.html'>u8</a><a class='primitive' href='https://doc.rust-lang.org/nightly/std/primitive.slice.html'>]</a>&gt;</span>","impl&lt;T&gt; <a class='trait' href='futures/future/trait.Future.html' title='futures::future::Future'>Future</a> for <a class='struct' href='tokio_core/net/udp/struct.RecvDgram.html' title='tokio_core::net::udp::RecvDgram'>RecvDgram</a>&lt;T&gt; <span class='where'>where T: <a class='trait' href='https://doc.rust-lang.org/nightly/core/convert/trait.AsMut.html' title='core::convert::AsMut'>AsMut</a>&lt;<a class='primitive' href='https://doc.rust-lang.org/nightly/std/primitive.slice.html'>[</a><a class='primitive' href='https://doc.rust-lang.org/nightly/std/primitive.u8.html'>u8</a><a class='primitive' href='https://doc.rust-lang.org/nightly/std/primitive.slice.html'>]</a>&gt;</span>","impl <a class='trait' href='futures/future/trait.Future.html' title='futures::future::Future'>Future</a> for <a class='struct' href='tokio_core/reactor/timeout/struct.Timeout.html' title='tokio_core::reactor::timeout::Timeout'>Timeout</a>","impl&lt;T&gt; <a class='trait' href='futures/future/trait.Future.html' title='futures::future::Future'>Future</a> for <a class='struct' href='tokio_proto/streaming/pipeline/advanced/struct.Pipeline.html' title='tokio_proto::streaming::pipeline::advanced::Pipeline'>Pipeline</a>&lt;T&gt; <span class='where'>where T: <a class='trait' href='tokio_proto/streaming/pipeline/advanced/trait.Dispatch.html' title='tokio_proto::streaming::pipeline::advanced::Dispatch'>Dispatch</a></span>","impl&lt;T&gt; <a class='trait' href='futures/future/trait.Future.html' title='futures::future::Future'>Future</a> for <a class='struct' href='tokio_proto/streaming/multiplex/advanced/struct.Multiplex.html' title='tokio_proto::streaming::multiplex::advanced::Multiplex'>Multiplex</a>&lt;T&gt; <span class='where'>where T: <a class='trait' href='tokio_proto/streaming/multiplex/advanced/trait.Dispatch.html' title='tokio_proto::streaming::multiplex::advanced::Dispatch'>Dispatch</a></span>","impl&lt;T, E&gt; <a class='trait' href='futures/future/trait.Future.html' title='futures::future::Future'>Future</a> for <a class='struct' href='tokio_proto/util/client_proxy/struct.Response.html' title='tokio_proto::util::client_proxy::Response'>Response</a>&lt;T, E&gt; <span class='where'>where E: <a class='trait' href='https://doc.rust-lang.org/nightly/core/convert/trait.From.html' title='core::convert::From'>From</a>&lt;<a class='struct' href='https://doc.rust-lang.org/nightly/std/io/error/struct.Error.html' title='std::io::error::Error'>Error</a>&gt;</span>","impl&lt;Kind, P&gt; <a class='trait' href='futures/future/trait.Future.html' title='futures::future::Future'>Future</a> for <a class='struct' href='tokio_proto/tcp_client/struct.Connect.html' title='tokio_proto::tcp_client::Connect'>Connect</a>&lt;Kind, P&gt; <span class='where'>where P: <a class='trait' href='tokio_proto/trait.BindClient.html' title='tokio_proto::BindClient'>BindClient</a>&lt;Kind, <a class='struct' href='tokio_core/net/tcp/struct.TcpStream.html' title='tokio_core::net::tcp::TcpStream'>TcpStream</a>&gt;</span>","impl <a class='trait' href='futures/future/trait.Future.html' title='futures::future::Future'>Future</a> for <a class='struct' href='hyper/client/struct.FutureResponse.html' title='hyper::client::FutureResponse'>FutureResponse</a>",];

            if (window.register_implementors) {
                window.register_implementors(implementors);
            } else {
                window.pending_implementors = implementors;
            }
        
})()
