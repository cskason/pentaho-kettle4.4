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
import org.pentaho.di.trans.Trans;

public class CleanupTransServletTest {
  private TransformationMap mockTransformationMap;

  private CleanupTransServlet cleanupTransServlet;

  @Before
  public void setup() {
    mockTransformationMap = mock(TransformationMap.class);
    cleanupTransServlet = new CleanupTransServlet(mockTransformationMap);
  }

  @Test
  public void testCleanupTransServletEscapesHtmlWhenTransNotFound() throws ServletException, IOException {
    HttpServletRequest mockHttpServletRequest = mock(HttpServletRequest.class);
    HttpServletResponse mockHttpServletResponse = mock(HttpServletResponse.class);
    StringWriter out = new StringWriter();
    PrintWriter printWriter = new PrintWriter(out);

    when(mockHttpServletRequest.getContextPath()).thenReturn(CleanupTransServlet.CONTEXT_PATH);
    when(mockHttpServletRequest.getParameter(anyString())).thenReturn(ServletTestUtils.BAD_STRING);
    when(mockHttpServletResponse.getWriter()).thenReturn(printWriter);

    cleanupTransServlet.doGet(mockHttpServletRequest, mockHttpServletResponse);

    assertFalse(ServletTestUtils.hasBadText(ServletTestUtils.getInsideOfTag("H1", out.toString())));
  }
  
  @Test
  public void testCleanupTransServletEscapesHtmlWhenTransFound() throws ServletException, IOException {
    HttpServletRequest mockHttpServletRequest = mock(HttpServletRequest.class);
    HttpServletResponse mockHttpServletResponse = mock(HttpServletResponse.class);
    Trans mockTrans = mock(Trans.class);
    StringWriter out = new StringWriter();
    PrintWriter printWriter = new PrintWriter(out);

    when(mockHttpServletRequest.getContextPath()).thenReturn(CleanupTransServlet.CONTEXT_PATH);
    when(mockHttpServletRequest.getParameter(anyString())).thenReturn(ServletTestUtils.BAD_STRING);
    when(mockHttpServletResponse.getWriter()).thenReturn(printWriter);
    when(mockTransformationMap.getTransformation(any(CarteObjectEntry.class))).thenReturn(mockTrans);

    cleanupTransServlet.doGet(mockHttpServletRequest, mockHttpServletResponse);
    assertFalse(ServletTestUtils.hasBadText(ServletTestUtils.getInsideOfTag("H1", out.toString())));
  }
}
