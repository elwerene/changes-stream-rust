(function() {var implementors = {};
implementors["lazy_static"] = [];implementors["void"] = [];implementors["libc"] = [];implementors["openssl"] = ["impl&lt;S:&nbsp;<a class='trait' href='https://doc.rust-lang.org/nightly/std/io/trait.Read.html' title='std::io::Read'>Read</a> + <a class='trait' href='https://doc.rust-lang.org/nightly/std/io/trait.Write.html' title='std::io::Write'>Write</a>&gt; <a class='trait' href='https://doc.rust-lang.org/nightly/std/io/trait.Read.html' title='std::io::Read'>Read</a> for <a class='struct' href='openssl/ssl/struct.SslStream.html' title='openssl::ssl::SslStream'>SslStream</a>&lt;S&gt;",];implementors["nix"] = [];implementors["native_tls"] = ["impl&lt;S:&nbsp;<a class='trait' href='https://doc.rust-lang.org/nightly/std/io/trait.Read.html' title='std::io::Read'>Read</a> + <a class='trait' href='https://doc.rust-lang.org/nightly/std/io/trait.Write.html' title='std::io::Write'>Write</a>&gt; <a class='trait' href='https://doc.rust-lang.org/nightly/std/io/trait.Read.html' title='std::io::Read'>Read</a> for <a class='struct' href='native_tls/struct.TlsStream.html' title='native_tls::TlsStream'>TlsStream</a>&lt;S&gt;",];implementors["mio"] = ["impl <a class='trait' href='https://doc.rust-lang.org/nightly/std/io/trait.Read.html' title='std::io::Read'>Read</a> for <a class='struct' href='mio/tcp/struct.TcpStream.html' title='mio::tcp::TcpStream'>TcpStream</a>","impl&lt;'a&gt; <a class='trait' href='https://doc.rust-lang.org/nightly/std/io/trait.Read.html' title='std::io::Read'>Read</a> for &amp;'a <a class='struct' href='mio/tcp/struct.TcpStream.html' title='mio::tcp::TcpStream'>TcpStream</a>","impl <a class='trait' href='https://doc.rust-lang.org/nightly/std/io/trait.Read.html' title='std::io::Read'>Read</a> for <a class='struct' href='mio/deprecated/unix/struct.Io.html' title='mio::deprecated::unix::Io'>Io</a>","impl&lt;'a&gt; <a class='trait' href='https://doc.rust-lang.org/nightly/std/io/trait.Read.html' title='std::io::Read'>Read</a> for &amp;'a <a class='struct' href='mio/deprecated/unix/struct.Io.html' title='mio::deprecated::unix::Io'>Io</a>","impl&lt;'a&gt; <a class='trait' href='https://doc.rust-lang.org/nightly/std/io/trait.Read.html' title='std::io::Read'>Read</a> for &amp;'a TcpStream","impl <a class='trait' href='https://doc.rust-lang.org/nightly/std/io/trait.Read.html' title='std::io::Read'>Read</a> for <a class='struct' href='mio/deprecated/unix/struct.UnixStream.html' title='mio::deprecated::unix::UnixStream'>UnixStream</a>","impl <a class='trait' href='https://doc.rust-lang.org/nightly/std/io/trait.Read.html' title='std::io::Read'>Read</a> for <a class='struct' href='mio/deprecated/unix/struct.PipeReader.html' title='mio::deprecated::unix::PipeReader'>PipeReader</a>","impl&lt;'a&gt; <a class='trait' href='https://doc.rust-lang.org/nightly/std/io/trait.Read.html' title='std::io::Read'>Read</a> for &amp;'a <a class='struct' href='mio/deprecated/unix/struct.PipeReader.html' title='mio::deprecated::unix::PipeReader'>PipeReader</a>",];implementors["tokio_core"] = ["impl <a class='trait' href='https://doc.rust-lang.org/nightly/std/io/trait.Read.html' title='std::io::Read'>Read</a> for <a class='struct' href='mio/net/tcp/struct.TcpStream.html' title='mio::net::tcp::TcpStream'>TcpStream</a>","impl&lt;'a&gt; <a class='trait' href='https://doc.rust-lang.org/nightly/std/io/trait.Read.html' title='std::io::Read'>Read</a> for &amp;'a <a class='struct' href='mio/net/tcp/struct.TcpStream.html' title='mio::net::tcp::TcpStream'>TcpStream</a>","impl <a class='trait' href='https://doc.rust-lang.org/nightly/std/io/trait.Read.html' title='std::io::Read'>Read</a> for <a class='struct' href='mio/sys/unix/io/struct.Io.html' title='mio::sys::unix::io::Io'>Io</a>","impl&lt;'a&gt; <a class='trait' href='https://doc.rust-lang.org/nightly/std/io/trait.Read.html' title='std::io::Read'>Read</a> for &amp;'a <a class='struct' href='mio/sys/unix/io/struct.Io.html' title='mio::sys::unix::io::Io'>Io</a>","impl&lt;'a&gt; <a class='trait' href='https://doc.rust-lang.org/nightly/std/io/trait.Read.html' title='std::io::Read'>Read</a> for &amp;'a TcpStream","impl <a class='trait' href='https://doc.rust-lang.org/nightly/std/io/trait.Read.html' title='std::io::Read'>Read</a> for <a class='struct' href='mio/deprecated/unix/struct.UnixStream.html' title='mio::deprecated::unix::UnixStream'>UnixStream</a>","impl <a class='trait' href='https://doc.rust-lang.org/nightly/std/io/trait.Read.html' title='std::io::Read'>Read</a> for <a class='struct' href='mio/deprecated/unix/struct.PipeReader.html' title='mio::deprecated::unix::PipeReader'>PipeReader</a>","impl&lt;'a&gt; <a class='trait' href='https://doc.rust-lang.org/nightly/std/io/trait.Read.html' title='std::io::Read'>Read</a> for &amp;'a <a class='struct' href='mio/deprecated/unix/struct.PipeReader.html' title='mio::deprecated::unix::PipeReader'>PipeReader</a>","impl&lt;T:&nbsp;<a class='trait' href='https://doc.rust-lang.org/nightly/std/io/trait.Read.html' title='std::io::Read'>Read</a>&gt; <a class='trait' href='https://doc.rust-lang.org/nightly/std/io/trait.Read.html' title='std::io::Read'>Read</a> for <a class='struct' href='tokio_core/io/struct.ReadHalf.html' title='tokio_core::io::ReadHalf'>ReadHalf</a>&lt;T&gt;","impl <a class='trait' href='https://doc.rust-lang.org/nightly/std/io/trait.Read.html' title='std::io::Read'>Read</a> for <a class='struct' href='tokio_core/net/struct.TcpStream.html' title='tokio_core::net::TcpStream'>TcpStream</a>","impl&lt;'a&gt; <a class='trait' href='https://doc.rust-lang.org/nightly/std/io/trait.Read.html' title='std::io::Read'>Read</a> for &amp;'a <a class='struct' href='tokio_core/net/struct.TcpStream.html' title='tokio_core::net::TcpStream'>TcpStream</a>","impl&lt;E:&nbsp;<a class='trait' href='https://doc.rust-lang.org/nightly/std/io/trait.Read.html' title='std::io::Read'>Read</a>&gt; <a class='trait' href='https://doc.rust-lang.org/nightly/std/io/trait.Read.html' title='std::io::Read'>Read</a> for <a class='struct' href='tokio_core/reactor/struct.PollEvented.html' title='tokio_core::reactor::PollEvented'>PollEvented</a>&lt;E&gt;","impl&lt;'a, E&gt; <a class='trait' href='https://doc.rust-lang.org/nightly/std/io/trait.Read.html' title='std::io::Read'>Read</a> for &amp;'a <a class='struct' href='tokio_core/reactor/struct.PollEvented.html' title='tokio_core::reactor::PollEvented'>PollEvented</a>&lt;E&gt; <span class='where'>where &amp;'a E: <a class='trait' href='https://doc.rust-lang.org/nightly/std/io/trait.Read.html' title='std::io::Read'>Read</a></span>",];implementors["tokio_tls"] = ["impl&lt;S:&nbsp;<a class='trait' href='tokio_core/io/trait.Io.html' title='tokio_core::io::Io'>Io</a>&gt; <a class='trait' href='https://doc.rust-lang.org/nightly/std/io/trait.Read.html' title='std::io::Read'>Read</a> for <a class='struct' href='tokio_tls/struct.TlsStream.html' title='tokio_tls::TlsStream'>TlsStream</a>&lt;S&gt;",];implementors["hyper"] = ["impl&lt;S&gt; <a class='trait' href='https://doc.rust-lang.org/nightly/std/io/trait.Read.html' title='std::io::Read'>Read</a> for <a class='struct' href='openssl/ssl/struct.SslStream.html' title='openssl::ssl::SslStream'>SslStream</a>&lt;S&gt; <span class='where'>where S: <a class='trait' href='https://doc.rust-lang.org/nightly/std/io/trait.Read.html' title='std::io::Read'>Read</a> + <a class='trait' href='https://doc.rust-lang.org/nightly/std/io/trait.Write.html' title='std::io::Write'>Write</a></span>","impl&lt;S&gt; <a class='trait' href='https://doc.rust-lang.org/nightly/std/io/trait.Read.html' title='std::io::Read'>Read</a> for <a class='struct' href='tokio_tls/struct.TlsStream.html' title='tokio_tls::TlsStream'>TlsStream</a>&lt;S&gt; <span class='where'>where S: <a class='trait' href='tokio_core/io/trait.Io.html' title='tokio_core::io::Io'>Io</a></span>","impl <a class='trait' href='https://doc.rust-lang.org/nightly/std/io/trait.Read.html' title='std::io::Read'>Read</a> for <a class='struct' href='mio/net/tcp/struct.TcpStream.html' title='mio::net::tcp::TcpStream'>TcpStream</a>","impl&lt;'a&gt; <a class='trait' href='https://doc.rust-lang.org/nightly/std/io/trait.Read.html' title='std::io::Read'>Read</a> for &amp;'a <a class='struct' href='mio/net/tcp/struct.TcpStream.html' title='mio::net::tcp::TcpStream'>TcpStream</a>","impl <a class='trait' href='https://doc.rust-lang.org/nightly/std/io/trait.Read.html' title='std::io::Read'>Read</a> for <a class='struct' href='mio/sys/unix/io/struct.Io.html' title='mio::sys::unix::io::Io'>Io</a>","impl&lt;'a&gt; <a class='trait' href='https://doc.rust-lang.org/nightly/std/io/trait.Read.html' title='std::io::Read'>Read</a> for &amp;'a <a class='struct' href='mio/sys/unix/io/struct.Io.html' title='mio::sys::unix::io::Io'>Io</a>","impl&lt;'a&gt; <a class='trait' href='https://doc.rust-lang.org/nightly/std/io/trait.Read.html' title='std::io::Read'>Read</a> for &amp;'a TcpStream","impl <a class='trait' href='https://doc.rust-lang.org/nightly/std/io/trait.Read.html' title='std::io::Read'>Read</a> for <a class='struct' href='mio/deprecated/unix/struct.UnixStream.html' title='mio::deprecated::unix::UnixStream'>UnixStream</a>","impl <a class='trait' href='https://doc.rust-lang.org/nightly/std/io/trait.Read.html' title='std::io::Read'>Read</a> for <a class='struct' href='mio/deprecated/unix/struct.PipeReader.html' title='mio::deprecated::unix::PipeReader'>PipeReader</a>","impl&lt;'a&gt; <a class='trait' href='https://doc.rust-lang.org/nightly/std/io/trait.Read.html' title='std::io::Read'>Read</a> for &amp;'a <a class='struct' href='mio/deprecated/unix/struct.PipeReader.html' title='mio::deprecated::unix::PipeReader'>PipeReader</a>","impl&lt;S&gt; <a class='trait' href='https://doc.rust-lang.org/nightly/std/io/trait.Read.html' title='std::io::Read'>Read</a> for <a class='struct' href='native_tls/struct.TlsStream.html' title='native_tls::TlsStream'>TlsStream</a>&lt;S&gt; <span class='where'>where S: <a class='trait' href='https://doc.rust-lang.org/nightly/std/io/trait.Read.html' title='std::io::Read'>Read</a> + <a class='trait' href='https://doc.rust-lang.org/nightly/std/io/trait.Write.html' title='std::io::Write'>Write</a></span>","impl&lt;T&gt; <a class='trait' href='https://doc.rust-lang.org/nightly/std/io/trait.Read.html' title='std::io::Read'>Read</a> for <a class='struct' href='tokio_core/io/split/struct.ReadHalf.html' title='tokio_core::io::split::ReadHalf'>ReadHalf</a>&lt;T&gt; <span class='where'>where T: <a class='trait' href='https://doc.rust-lang.org/nightly/std/io/trait.Read.html' title='std::io::Read'>Read</a></span>","impl <a class='trait' href='https://doc.rust-lang.org/nightly/std/io/trait.Read.html' title='std::io::Read'>Read</a> for <a class='struct' href='tokio_core/net/tcp/struct.TcpStream.html' title='tokio_core::net::tcp::TcpStream'>TcpStream</a>","impl&lt;'a&gt; <a class='trait' href='https://doc.rust-lang.org/nightly/std/io/trait.Read.html' title='std::io::Read'>Read</a> for &amp;'a <a class='struct' href='tokio_core/net/tcp/struct.TcpStream.html' title='tokio_core::net::tcp::TcpStream'>TcpStream</a>","impl&lt;E&gt; <a class='trait' href='https://doc.rust-lang.org/nightly/std/io/trait.Read.html' title='std::io::Read'>Read</a> for <a class='struct' href='tokio_core/reactor/poll_evented/struct.PollEvented.html' title='tokio_core::reactor::poll_evented::PollEvented'>PollEvented</a>&lt;E&gt; <span class='where'>where E: <a class='trait' href='https://doc.rust-lang.org/nightly/std/io/trait.Read.html' title='std::io::Read'>Read</a></span>","impl&lt;'a, E&gt; <a class='trait' href='https://doc.rust-lang.org/nightly/std/io/trait.Read.html' title='std::io::Read'>Read</a> for &amp;'a <a class='struct' href='tokio_core/reactor/poll_evented/struct.PollEvented.html' title='tokio_core::reactor::poll_evented::PollEvented'>PollEvented</a>&lt;E&gt; <span class='where'>where &amp;'a E: <a class='trait' href='https://doc.rust-lang.org/nightly/std/io/trait.Read.html' title='std::io::Read'>Read</a></span>",];

            if (window.register_implementors) {
                window.register_implementors(implementors);
            } else {
                window.pending_implementors = implementors;
            }
        
})()
