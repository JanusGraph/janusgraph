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
  <xsl:import  href="single.xsl"/>
  <xsl:import  href="docbook/html/chunk-common.xsl"/>
  <xsl:include href="docbook/html/chunk-code.xsl"/>
  
  <xsl:param name="base.dir">$MAVEN{htmlchunk.output.dir}</xsl:param>
  
  <xsl:param name="janusgraph.top.nav.links" select="1" />

  <xsl:param name="generate.toc">
  book         toc,title
  book/part    toc,title
  book/part/chapter toc,title
  </xsl:param>
  
  <xsl:template match="figure[@role = 'tss-centeredfig']" mode="class.value">
    <xsl:value-of select="'tss-centeredfig'"/>
  </xsl:template>

  <xsl:template match="informaltable[@role = 'tss-config-table']" mode="class.value">
    <xsl:value-of select="'tss-config-table'"/>
  </xsl:template>

  <xsl:template name="chunk-element-content">
    <xsl:param name="prev"/>
    <xsl:param name="next"/>
    <xsl:param name="nav.context"/>
    <xsl:param name="content">
      <xsl:apply-imports/>
    </xsl:param>
    <xsl:param name="navheader">  
<!--
        <xsl:call-template name="janusgraph.header.navigation">
          <xsl:with-param name="prev" select="$prev"/>
          <xsl:with-param name="next" select="$next"/>
          <xsl:with-param name="nav.context" select="$nav.context"/>
        </xsl:call-template>

        <xsl:call-template name="user.header.navigation">
          <xsl:with-param name="prev" select="$prev"/>
          <xsl:with-param name="next" select="$next"/>
          <xsl:with-param name="nav.context" select="$nav.context"/>
        </xsl:call-template>
-->
        <xsl:call-template name="janusgraph.header.navigation">
          <xsl:with-param name="prev" select="$prev"/>
          <xsl:with-param name="next" select="$next"/>
          <xsl:with-param name="nav.context" select="$nav.context"/>
        </xsl:call-template>
    </xsl:param>
    <xsl:param name="navfooter">
      <xsl:call-template name="footer.navigation">
        <xsl:with-param name="prev" select="$prev"/>
        <xsl:with-param name="next" select="$next"/>
        <xsl:with-param name="nav.context" select="$nav.context"/>
      </xsl:call-template>
  
      <xsl:call-template name="user.footer.navigation">
        <xsl:with-param name="prev" select="$prev"/>
        <xsl:with-param name="next" select="$next"/>
        <xsl:with-param name="nav.context" select="$nav.context"/>
      </xsl:call-template>
    </xsl:param>
  
    <xsl:call-template name="user.preroot"/>
  
    <html>
      <xsl:call-template name="root.attributes"/>

      <xsl:call-template name="html.head">
        <xsl:with-param name="prev" select="$prev"/>
        <xsl:with-param name="next" select="$next"/>
      </xsl:call-template>
  
      <xsl:call-template name="janusgraph.body">
        <xsl:with-param name="headercontent" select="$navheader"/>
        <xsl:with-param name="maincontent"   select="$content"/>
        <xsl:with-param name="footercontent" select="$navfooter"/>
      </xsl:call-template>
    </html>
    <xsl:value-of select="$chunk.append"/>
  </xsl:template>
  
</xsl:stylesheet>
