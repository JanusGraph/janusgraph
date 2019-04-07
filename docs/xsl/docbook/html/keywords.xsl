<?xml version='1.0'?>
<xsl:stylesheet exclude-result-prefixes="d"
                 xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
                xmlns:d="https://docbook.org/ns/docbook"
version='1.0'>

<!-- ********************************************************************
     $Id: keywords.xsl 6910 2007-06-28 23:23:30Z xmldoc $
     ********************************************************************

     This file is part of the XSL DocBook Stylesheet distribution.
     See ../README or http://docbook.sf.net/release/xsl/current/ for
     copyright and other information.

     ******************************************************************** -->

<xsl:template match="d:keywordset"></xsl:template>
<xsl:template match="d:subjectset"></xsl:template>

<!-- ==================================================================== -->

<xsl:template match="d:keywordset" mode="html.header">
  <meta name="keywords">
    <xsl:attribute name="content">
      <xsl:apply-templates select="d:keyword" mode="html.header"/>
    </xsl:attribute>
  </meta>
</xsl:template>

<xsl:template match="d:keyword" mode="html.header">
  <xsl:apply-templates/>
  <xsl:if test="following-sibling::d:keyword">, </xsl:if>
</xsl:template>

<!-- ==================================================================== -->

</xsl:stylesheet>
