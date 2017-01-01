(function() {var implementors = {};
implementors["futures"] = [];implementors["tokio_core"] = ["impl&lt;T:&nbsp;<a class='trait' href='tokio_core/io/trait.Io.html' title='tokio_core::io::Io'>Io</a>, C:&nbsp;<a class='trait' href='tokio_core/io/trait.Codec.html' title='tokio_core::io::Codec'>Codec</a>&gt; <a class='trait' href='futures/stream/trait.Stream.html' title='futures::stream::Stream'>Stream</a> for <a class='struct' href='tokio_core/io/struct.Framed.html' title='tokio_core::io::Framed'>Framed</a>&lt;T, C&gt;","impl&lt;T&gt; <a class='trait' href='futures/stream/trait.Stream.html' title='futures::stream::Stream'>Stream</a> for <a class='struct' href='tokio_core/channel/struct.Receiver.html' title='tokio_core::channel::Receiver'>Receiver</a>&lt;T&gt;","impl <a class='trait' href='futures/stream/trait.Stream.html' title='futures::stream::Stream'>Stream</a> for <a class='struct' href='tokio_core/net/struct.Incoming.html' title='tokio_core::net::Incoming'>Incoming</a>","impl&lt;C:&nbsp;<a class='trait' href='tokio_core/net/trait.UdpCodec.html' title='tokio_core::net::UdpCodec'>UdpCodec</a>&gt; <a class='trait' href='futures/stream/trait.Stream.html' title='futures::stream::Stream'>Stream</a> for <a class='struct' href='tokio_core/net/struct.UdpFramed.html' title='tokio_core::net::UdpFramed'>UdpFramed</a>&lt;C&gt;","impl <a class='trait' href='futures/stream/trait.Stream.html' title='futures::stream::Stream'>Stream</a> for <a class='struct' href='tokio_core/reactor/struct.Interval.html' title='tokio_core::reactor::Interval'>Interval</a>",];implementors["tokio_proto"] = ["impl&lt;T, E&gt; <a class='trait' href='futures/stream/trait.Stream.html' title='futures::stream::Stream'>Stream</a> for <a class='struct' href='tokio_proto/streaming/struct.Body.html' title='tokio_proto::streaming::Body'>Body</a>&lt;T, E&gt;",];implementors["hyper"] = ["impl&lt;T, C&gt; <a class='trait' href='futures/stream/trait.Stream.html' title='futures::stream::Stream'>Stream</a> for <a class='struct' href='tokio_core/io/frame/struct.Framed.html' title='tokio_core::io::frame::Framed'>Framed</a>&lt;T, C&gt; <span class='where'>where C: <a class='trait' href='tokio_core/io/frame/trait.Codec.html' title='tokio_core::io::frame::Codec'>Codec</a>, T: <a class='trait' href='tokio_core/io/trait.Io.html' title='tokio_core::io::Io'>Io</a></span>","impl&lt;T&gt; <a class='trait' href='futures/stream/trait.Stream.html' title='futures::stream::Stream'>Stream</a> for <a class='struct' href='tokio_core/channel/struct.Receiver.html' title='tokio_core::channel::Receiver'>Receiver</a>&lt;T&gt;","impl <a class='trait' href='futures/stream/trait.Stream.html' title='futures::stream::Stream'>Stream</a> for <a class='struct' href='tokio_core/net/tcp/struct.Incoming.html' title='tokio_core::net::tcp::Incoming'>Incoming</a>","impl&lt;C&gt; <a class='trait' href='futures/stream/trait.Stream.html' title='futures::stream::Stream'>Stream</a> for <a class='struct' href='tokio_core/net/udp/frame/struct.UdpFramed.html' title='tokio_core::net::udp::frame::UdpFramed'>UdpFramed</a>&lt;C&gt; <span class='where'>where C: <a class='trait' href='tokio_core/net/udp/frame/trait.UdpCodec.html' title='tokio_core::net::udp::frame::UdpCodec'>UdpCodec</a></span>","impl <a class='trait' href='futures/stream/trait.Stream.html' title='futures::stream::Stream'>Stream</a> for <a class='struct' href='tokio_core/reactor/interval/struct.Interval.html' title='tokio_core::reactor::interval::Interval'>Interval</a>","impl&lt;T, E&gt; <a class='trait' href='futures/stream/trait.Stream.html' title='futures::stream::Stream'>Stream</a> for <a class='struct' href='tokio_proto/streaming/body/struct.Body.html' title='tokio_proto::streaming::body::Body'>Body</a>&lt;T, E&gt;","impl <a class='trait' href='futures/stream/trait.Stream.html' title='futures::stream::Stream'>Stream</a> for <a class='struct' href='hyper/struct.Body.html' title='hyper::Body'>Body</a>",];

            if (window.register_implementors) {
                window.register_implementors(implementors);
            } else {
                window.pending_implementors = implementors;
            }
        
})()
