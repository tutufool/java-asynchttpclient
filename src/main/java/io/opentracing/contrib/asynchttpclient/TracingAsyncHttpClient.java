package io.opentracing.contrib.asynchttpclient;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.asynchttpclient.AsyncCompletionHandlerBase;
import org.asynchttpclient.AsyncHandler;
import org.asynchttpclient.AsyncHttpClientConfig;
import org.asynchttpclient.DefaultAsyncHttpClient;
import org.asynchttpclient.DefaultAsyncHttpClientConfig;
import org.asynchttpclient.HttpResponseBodyPart;
import org.asynchttpclient.HttpResponseStatus;
import org.asynchttpclient.ListenableFuture;
import org.asynchttpclient.Request;
import org.asynchttpclient.RequestBuilder;
import org.asynchttpclient.Response;

import io.netty.handler.codec.http.HttpHeaders;
import io.opentracing.Scope;
import io.opentracing.Span;
import io.opentracing.Tracer;
import io.opentracing.propagation.Format;
import io.opentracing.propagation.TextMap;
import io.opentracing.util.GlobalTracer;

/**
 * An {@link org.asynchttpclient.AsyncHttpClient} that traces HTTP calls using
 * the OpenTracing API.
 */
@SuppressWarnings("unused")
public class TracingAsyncHttpClient extends DefaultAsyncHttpClient {

	private Tracer tracer;
	private SpanDecorator spanDecorator;
	private List<CompletableFuture<?>> traceFutures;

	public TracingAsyncHttpClient(SpanDecorator spanDecorator) {
		this(GlobalTracer.get(), spanDecorator);
	}

	public TracingAsyncHttpClient(Tracer tracer, SpanDecorator spanDecorator) {
		this(tracer, spanDecorator, new DefaultAsyncHttpClientConfig.Builder().build());
	}

	public TracingAsyncHttpClient(Tracer tracer, SpanDecorator spanDecorator, AsyncHttpClientConfig config) {
		super(config);
		this.tracer = tracer;
		this.spanDecorator = spanDecorator;
		// Retain a reference to futures so they aren't GC'ed before completion.
		this.traceFutures = new ArrayList<>();
	}

	@Override
	public ListenableFuture<Response> executeRequest(Request request) {
		return executeRequest(request, new AsyncCompletionHandlerBase());
	}

	@Override
	public ListenableFuture<Response> executeRequest(RequestBuilder builder) {
		return executeRequest(builder.build());
	}

	@Override
	public <T> ListenableFuture<T> executeRequest(Request request, AsyncHandler<T> handler) {
		return internalExecuteRequest(request, handler);
	}

	@Override
	public <T> ListenableFuture<T> executeRequest(RequestBuilder builder, AsyncHandler<T> handler) {
		return executeRequest(builder.build(), handler);
	}

	private <T> ListenableFuture<T> internalExecuteRequest(Request request, AsyncHandler<T> handler) {

		Tracer.SpanBuilder builder = tracer.buildSpan(spanDecorator.getOperationName(request));

		Span parent = tracer.activeSpan();
		if (parent != null) {
			System.out.println("ACTIVE_SPAN_LIVE"); // TODO: REMOVE
			builder.asChildOf(parent);
		} else { // TODO: REMOVE
			System.out.println("ACTIVE_SPAN_NULL");
		}

		spanDecorator.decorateSpan(builder, request);

		final Span span = builder.start();
		RequestBuilder requestBuilder = new RequestBuilder(request);

		tracer.inject(span.context(), Format.Builtin.HTTP_HEADERS, new TextMap() {
			@Override
			public Iterator<Map.Entry<String, String>> iterator() {
				throw new UnsupportedOperationException("iterator should never be used with Tracer.inject()");
			}

			@Override
			public void put(String key, String value) {
				requestBuilder.addHeader(key, value);
			}
		});
		// by activate span, RequestFilter can be aware of active span.
		// in ResponseFilter and IOExceptionFilter, we can cast handler to Traceable
		try (Scope scope = tracer.scopeManager().activate(span, false)) {
			ListenableFuture<T> lf = super.executeRequest(requestBuilder.build(),
					new SpanAwareAsyncHandler<T>(handler, span, tracer));
			traceFutures.add(lf.toCompletableFuture().whenComplete((t, throwable) -> {
				span.finish();//This should be later than onCompleted???
				clearFinished(traceFutures);
			}));

			return lf;
		}
	}

	private static void clearFinished(List<CompletableFuture<?>> futures) {
		Iterator<CompletableFuture<?>> iter = futures.iterator();
		while (iter.hasNext()) {
			if (iter.next().isDone()) {
				iter.remove();
			}
		}
	}

	/**
	 * An Abstract API that allows the TracingAsyncHttpClient to customize how spans
	 * are named and decorated.
	 */

	public interface SpanDecorator {
		/**
		 * @param request
		 *            the request that a span is being constructed for.
		 * @return the operation name a span should use for this request.
		 */
		String getOperationName(Request request);

		/**
		 * Adds data to a span based on the contents of the request.
		 * 
		 * @param span
		 *            the span for an operation.
		 * @param request
		 *            the request that represents the operation.
		 */
		void decorateSpan(Tracer.SpanBuilder span, Request request);

		@SuppressWarnings("unused")
		SpanDecorator DEFAULT = new SpanDecorator() {
			@Override
			public String getOperationName(Request request) {
				return request.getUri().getHost() + ":" + request.getMethod();
			}

			@Override
			public void decorateSpan(Tracer.SpanBuilder span, Request request) {
				// Be default, do nothing.
			}
		};
	}

	public interface Traceable {

		Span getSpan();

		Tracer getTracer();

	}

	private static class SpanAwareAsyncHandler<T> implements AsyncHandler<T>, Traceable {

		private final AsyncHandler<T> delegate;
		private final Span span;
		private final Tracer tracer;

		SpanAwareAsyncHandler(AsyncHandler<T> delegate, Span span, Tracer tracer) {
			this.delegate = delegate;
			this.span = span;
			this.tracer = tracer;
		}

		@Override
		public Span getSpan() {
			return span;
		}

		@Override
		public Tracer getTracer() {
			return tracer;
		}

		@Override
		public State onStatusReceived(HttpResponseStatus responseStatus) throws Exception {
			try (Scope socpe = tracer.scopeManager().activate(span, false)) {
				return delegate.onStatusReceived(responseStatus);
			}
		}

		@Override
		public State onHeadersReceived(HttpHeaders headers) throws Exception {
			try (Scope socpe = tracer.scopeManager().activate(span, false)) {
				return delegate.onHeadersReceived(headers);
			}
		}

		@Override
		public State onBodyPartReceived(HttpResponseBodyPart bodyPart) throws Exception {
			try (Scope socpe = tracer.scopeManager().activate(span, false)) {
				return delegate.onBodyPartReceived(bodyPart);
			}
		}

		@Override
		public void onThrowable(Throwable t) {
			try (Scope socpe = tracer.scopeManager().activate(span, false)) {
				delegate.onThrowable(t);
			}
		}

		@Override
		public T onCompleted() throws Exception {
			try (Scope socpe = tracer.scopeManager().activate(span, false)) {
				return delegate.onCompleted();
			}
		}

	}

}
