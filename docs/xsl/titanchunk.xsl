<!--
  Generates single XHTML document from DocBook XML source using DocBook XSL
  stylesheets.

  NOTE: The URL reference to the current DocBook XSL stylesheets is
  rewritten to point to the copy on the local disk drive by the XML catalog
  rewrite directives so it doesn't need to go out to the Internet for the
  stylesheets. This means you don't need to edit the <xsl:import> elements on
  a machine by machine basis.
-->
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="1.0"
    xmlns:sbhl="java:net.sf.xslthl.ConnectorSaxonB"
    xmlns:xslthl="http://xslthl.sf.net"
    extension-element-prefixes="sbhl xslthl">

<!-- Importing our customization of docbook.xsl in accordance with the
     comment at the top of chunk.xsl -->
<xsl:import href="http://docbook.sourceforge.net/release/xsl-ns/current/html/highlight.xsl"/>
<xsl:import href="common.xsl"/>
<xsl:import href="titansingle.xsl"/>
<xsl:import href="http://docbook.sourceforge.net/release/xsl-ns/current/html/chunk-common.xsl"/>
<xsl:include href="http://docbook.sourceforge.net/release/xsl-ns/current/html/chunk-code.xsl"/>

<!-- Output directory for chunks -->
<xsl:param name="base.dir">target/docs/htmlchunk</xsl:param>

</xsl:stylesheet>
