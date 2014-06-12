<?xml version="1.0"?>
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
                xmlns:d="http://docbook.org/ns/docbook"
                xmlns:xslthl="http://xslthl.sf.net"
                exclude-result-prefixes="xslthl d" version="1.0">

  <xsl:import href="../docbook/html/highlight.xsl"/>

  <xsl:param name="highlight.xslthl.config">file://$MAVEN{xsl.output.dir}/highlight/xslthl-config.xml</xsl:param>

  <xsl:template match="xslthl:keyword" mode="xslthl">
    <strong class="hl-keyword">
      <xsl:apply-templates mode="xslthl"/>
    </strong>
  </xsl:template>

  <xsl:template match="xslthl:attribute" mode="xslthl">
    <span class="hl-attribute">
      <xsl:apply-templates mode="xslthl"/>
    </span>
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

  <xsl:template match="xslthl:repl-prompt" mode="xslthl">
    <span class="hl-repl-prompt">
      <xsl:apply-templates mode="xslthl"/>
    </span>
  </xsl:template>

  <xsl:template match="xslthl:repl-result" mode="xslthl">
    <span class="hl-repl-result">
      <xsl:apply-templates mode="xslthl"/>
    </span>
  </xsl:template>

  <xsl:template match="xslthl:gremlin-func" mode="xslthl">
    <span class="hl-gremlin-func">
      <xsl:apply-templates mode="xslthl"/>
    </span>
  </xsl:template>

</xsl:stylesheet>

