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

  <xsl:import href="docbook/html/docbook.xsl"/>
  <xsl:import href="highlight/highlight.xsl"/>
  <xsl:import href="common.xsl"/>
  
  <xsl:output
      method="html"
      encoding="utf-8"
      doctype-public="-//W3C//DTD HTML 4.01 Transitional//EN"
      doctype-system="http://www.w3.org/TR/html4/loose.dtd"
      indent="yes" />
  
  <xsl:param name="generate.toc">
  book         toc,title
  book/part    title
  book/chapter title
  </xsl:param>
  
  <xsl:template name="user.head.content">
    <xsl:call-template name="janusgraph.head"/>
  </xsl:template>
  
  <xsl:template match="*" mode="process.root">
    <xsl:variable name="doc" select="self::*"/>
    <xsl:variable name="content">
      <xsl:apply-imports/>
    </xsl:variable>
  
    <!--
    <xsl:call-template name="user.preroot"/>
    <xsl:call-template name="root.messages"/>
    -->
  
    <html>
      <xsl:call-template name="root.attributes"/>
      <head>
        <xsl:call-template name="system.head.content">
          <xsl:with-param name="node" select="$doc"/>
        </xsl:call-template>
        <xsl:call-template name="head.content">
          <xsl:with-param name="node" select="$doc"/>
        </xsl:call-template>
        <xsl:call-template name="user.head.content">
          <xsl:with-param name="node" select="$doc"/>
        </xsl:call-template>
      </head>
  
      <xsl:call-template name="janusgraph.body">
        <xsl:with-param name="maincontent" select="$content"/>
      </xsl:call-template>
    </html>
  
    <xsl:value-of select="$html.append"/>
  
    <!-- Generate any css files only once, not once per chunk -->
    <xsl:call-template name="generate.css.files"/>
  </xsl:template>
  
  <!--
  <xsl:template match='xslthl:keyword'>
     <b class="hl-keyword"><xsl:apply-templates mode="xslthl"/></b>
  </xsl:template>
  
  <xsl:template match='xslthl:comment'>
     <i class="hl-comment" style="color: black"><xsl:apply-templates mode="xslthl"/></i>
  </xsl:template>
  
  <xsl:template match="xslthl:attribute" mode="xslthl">
    <span class="hl-attribute" style="color: #F5844C"><xsl:apply-templates mode="xslthl"/></span>
  </xsl:template>
  
  <xsl:template match="xslthl:string" mode="xslthl">
    <b class="hl-string"><xsl:apply-templates mode="xslthl"/></b>
  </xsl:template>
  
  <xsl:template match="xslthl:value" mode="xslthl">
    <span class="hl-value" style="color: #993300"><xsl:apply-templates mode="xslthl"/></span>
  </xsl:template>
  -->
  
</xsl:stylesheet>
