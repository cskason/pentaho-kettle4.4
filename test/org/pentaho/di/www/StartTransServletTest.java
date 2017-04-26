package org.pentaho.di.www;

import static junit.framework.Assert.assertFalse;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.junit.Before;
import org.junit.Test;
import org.pentaho.di.core.gui.Point;
import org.pentaho.di.core.logging.CentralLogStore;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;

public class StartTransServletTest {
  private TransformationMap mockTransformationMap;

  private StartTransServlet startTransServlet;

  @Before
  public void setup() {
    mockTransformationMap = mock(TransformationMap.class);
    startTransServlet = new StartTransServlet(mockTransformationMap);
  }

  @Test
  public void testStartTransServletEscapesHtmlWhenTransNotFound() throws ServletException, IOException {
    HttpServletRequest mockHttpServletRequest = mock(HttpServletRequest.class);
    HttpServletResponse mockHttpServletResponse = mock(HttpServletResponse.class);

    StringWriter out = new StringWriter();
    PrintWriter printWriter = new PrintWriter(out);

    when(mockHttpServletRequest.getContextPath()).thenReturn(StartTransServlet.CONTEXT_PATH);
    when(mockHttpServletRequest.getParameter(anyString())).thenReturn(ServletTestUtils.BAD_STRING);
    when(mockHttpServletResponse.getWriter()).thenReturn(printWriter);

    startTransServlet.doGet(mockHttpServletRequest, mockHttpServletResponse);
    assertFalse(ServletTestUtils.hasBadText(ServletTestUtils.getInsideOfTag("H1", out.toString())));
  }

  @Test
  public void testStartTransServletEscapesHtmlWhenTransFound() throws ServletException, IOException {
    CentralLogStore.init();
    HttpServletRequest mockHttpServletRequest = mock(HttpServletRequest.class);
    HttpServletResponse mockHttpServletResponse = mock(HttpServletResponse.class);
    Trans mockTrans = mock(Trans.class);
    TransMeta mockTransMeta = mock(TransMeta.class);
    LogChannelInterface mockChannelInterface = mock(LogChannelInterface.class);
    StringWriter out = new StringWriter();
    PrintWriter printWriter = new PrintWriter(out);

    when(mockHttpServletRequest.getContextPath()).thenReturn(StartTransServlet.CONTEXT_PATH);
    when(mockHttpServletRequest.getParameter(anyString())).thenReturn(ServletTestUtils.BAD_STRING);
    when(mockHttpServletResponse.getWriter()).thenReturn(printWriter);
    when(mockTransformationMap.getTransformation(any(CarteObjectEntry.class))).thenReturn(mockTrans);
    when(mockTrans.getLogChannel()).thenReturn(mockChannelInterface);
    when(mockTrans.getLogChannelId()).thenReturn("test");
    when(mockTrans.getTransMeta()).thenReturn(mockTransMeta);
    when(mockTransMeta.getMaximum()).thenReturn(new Point(10, 10));

    startTransServlet.doGet(mockHttpServletRequest, mockHttpServletResponse);
    assertFalse(ServletTestUtils.hasBadText(ServletTestUtils.getInsideOfTag("H1", out.toString())));
  }
}
