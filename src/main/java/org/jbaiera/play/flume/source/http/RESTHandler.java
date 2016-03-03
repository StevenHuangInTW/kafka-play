package org.jbaiera.play.flume.source.http;

import com.google.common.io.CharStreams;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.http.HTTPBadRequestException;
import org.apache.flume.source.http.HTTPSourceHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.*;

/**
 * HTTP Handler for Flume that preserves the path and information pertaining to the request.
 */
public class RESTHandler implements HTTPSourceHandler {

	private static final Logger LOGGER = LoggerFactory.getLogger(RESTHandler.class);

	public static final String METHOD = "flume.rest.method";
	public static final String URI = "flume.rest.uri";
	public static final String QUERY = "flume.rest.query";

	@Override
	public List<Event> getEvents(HttpServletRequest request) throws HTTPBadRequestException, IOException {
		Map<String, String> headers = getHeaders(request);
		try (InputStream in = request.getInputStream()) {
			Charset charset = getRequestCharset(request);
			byte[] body = CharStreams.toString(new InputStreamReader(in, charset)).getBytes();
			Event event = EventBuilder.withBody(body, headers);
			LOGGER.debug("blobEvent: {}", event);
			return Collections.singletonList(event);
		}
	}

	private Charset getRequestCharset(HttpServletRequest request) throws HTTPBadRequestException {
		if (request.getCharacterEncoding() != null) {
			if (Charset.isSupported(request.getCharacterEncoding())) {
				return Charset.forName(request.getCharacterEncoding());
			}
			else {
				throw new HTTPBadRequestException("Unsupported Charset '"+request.getCharacterEncoding()+"'");
			}
		}
		else {
			return Charset.defaultCharset();
		}
	}

	private Map<String, String> getHeaders(HttpServletRequest request) {
		if (LOGGER.isDebugEnabled()) {
			Map<Object, Object> requestHeaders = new HashMap<>();
			Enumeration iter = request.getHeaderNames();
			while (iter.hasMoreElements()) {
				String name = (String) iter.nextElement();
				requestHeaders.put(name, request.getHeader(name));
			}
			LOGGER.debug("requestHeaders: {}", requestHeaders);
		}
		Map<String, String> headers = new HashMap<>();
		if (request.getContentType() != null) {
			headers.put("Content-Type", request.getContentType());
		}

		if (request.getMethod() != null) {
			headers.put(METHOD, request.getMethod());
		}
		if (request.getRequestURI() != null) {
			headers.put(URI, request.getRequestURI());
		}
		if (request.getQueryString() != null) {
			headers.put(QUERY, request.getQueryString());
		}

		Enumeration iter = request.getParameterNames();
		while (iter.hasMoreElements()) {
			String name = (String) iter.nextElement();
			headers.put(name, request.getParameter(name));
		}
		return headers;
	}

	@Override
	public void configure(Context context) {

	}
}
