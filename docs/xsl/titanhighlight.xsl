<?xml version="1.0"?>
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
                xmlns:d="http://docbook.org/ns/docbook"
                xmlns:xslthl="http://xslthl.sf.net"
                exclude-result-prefixes="xslthl d" version="1.0">

  <xsl:import href="http://docbook.sourceforge.net/release/xsl-ns/current/html/highlight.xsl"/>

  <xsl:template match="xslthl:keyword" mode="xslthl">
    <strong class="hl-keyword">
      <xsl:apply-templates mode="xslthl"/>
    </strong>
  </xsl:template>

  <xsl:template match="xslthl:string" mode="xslthl">
    <span class="hl-string">
      <xsl:apply-templates mode="xslthl"/>
    </span>
  </xsl:template>

  <xsl:template match="xslthl:comment" mode="xslthl">
    <em class="hl-comment">
      <xsl:apply-templates mode="xslthl"/>
    </em>
  </xsl:template>

</xsl:stylesheet>

