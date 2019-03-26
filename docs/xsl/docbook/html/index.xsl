<?xml version='1.0'?>
<xsl:stylesheet exclude-result-prefixes="d"
                 xmlns:xsl="https://www.w3.org/1999/XSL/Transform"
                xmlns:d="https://docbook.org/ns/docbook"
version='1.0'>

<!-- ********************************************************************
     $Id: index.xsl 9297 2012-04-22 03:56:16Z bobstayton $
     ********************************************************************

     This file is part of the XSL DocBook Stylesheet distribution.
     See ../README or http://docbook.sf.net/release/xsl/current/ for
     copyright and other information.

     ******************************************************************** -->

<!-- ==================================================================== -->

<xsl:template match="d:index">
  <!-- some implementations use completely empty index tags to indicate -->
  <!-- where an automatically generated index should be inserted. so -->
  <!-- if the index is completely empty, skip it. Unless generate.index -->
  <!-- is non-zero, in which case, this is where the automatically -->
  <!-- generated index should go. -->

  <xsl:call-template name="id.warning"/>

  <xsl:if test="count(*)>0 or $generate.index != '0'">
    <div>
      <xsl:apply-templates select="." mode="common.html.attributes"/>
      <xsl:call-template name="id.attribute">
        <xsl:with-param name="conditional" select="0"/>
      </xsl:call-template>

      <xsl:call-template name="index.titlepage"/>
      <xsl:choose>
	<xsl:when test="d:indexdiv">
	  <xsl:apply-templates/>
	</xsl:when>
	<xsl:otherwise>
	  <xsl:apply-templates select="*[not(self::d:indexentry)]"/>
	  <!-- Because it's actually valid for Index to have neither any -->
	  <!-- Indexdivs nor any Indexentries, we need to check and make -->
	  <!-- sure that at least one Indexentry exists, and generate a -->
	  <!-- wrapper dl if there is at least one; otherwise, do nothing. -->
	  <xsl:if test="d:indexentry">
	    <!-- The indexentry template assumes a parent dl wrapper has -->
	    <!-- been generated; for Indexes that have Indexdivs, the dl -->
	    <!-- wrapper is generated by the indexdiv template; however, -->
	    <!-- for Indexes that lack Indexdivs, if we don't generate a -->
	    <!-- dl here, HTML output will not be valid. -->
	    <dl>
	      <xsl:apply-templates select="d:indexentry"/>
	    </dl>
	  </xsl:if>
	</xsl:otherwise>
      </xsl:choose>

      <xsl:if test="count(d:indexentry) = 0 and count(d:indexdiv) = 0">
        <xsl:call-template name="generate-index">
          <xsl:with-param name="scope" select="(ancestor::d:book|/)[last()]"/>
        </xsl:call-template>
      </xsl:if>

      <xsl:if test="not(parent::d:article)">
        <xsl:call-template name="process.footnotes"/>
      </xsl:if>
    </div>
  </xsl:if>
</xsl:template>

<xsl:template match="d:setindex">
  <!-- some implementations use completely empty index tags to indicate -->
  <!-- where an automatically generated index should be inserted. so -->
  <!-- if the index is completely empty, skip it. Unless generate.index -->
  <!-- is non-zero, in which case, this is where the automatically -->
  <!-- generated index should go. -->

  <xsl:call-template name="id.warning"/>

  <xsl:if test="count(*)>0 or $generate.index != '0'">
    <div>
      <xsl:apply-templates select="." mode="common.html.attributes"/>
      <xsl:call-template name="id.attribute">
        <xsl:with-param name="conditional" select="0"/>
      </xsl:call-template>

      <xsl:call-template name="setindex.titlepage"/>
      <xsl:apply-templates/>

      <xsl:if test="count(d:indexentry) = 0 and count(d:indexdiv) = 0">
        <xsl:call-template name="generate-index">
          <xsl:with-param name="scope" select="/"/>
        </xsl:call-template>
      </xsl:if>

      <xsl:if test="not(parent::d:article)">
        <xsl:call-template name="process.footnotes"/>
      </xsl:if>
    </div>
  </xsl:if>
</xsl:template>

<xsl:template match="d:index/d:indexinfo"></xsl:template>
<xsl:template match="d:index/d:info"></xsl:template>
<xsl:template match="d:index/d:title"></xsl:template>
<xsl:template match="d:index/d:subtitle"></xsl:template>
<xsl:template match="d:index/d:titleabbrev"></xsl:template>

<!-- ==================================================================== -->

<xsl:template match="d:indexdiv">
  <xsl:call-template name="id.warning"/>

  <div>
    <xsl:apply-templates select="." mode="common.html.attributes"/>
    <xsl:call-template name="id.attribute"/>
    <xsl:call-template name="anchor"/>
    <xsl:apply-templates select="*[not(self::d:indexentry)]"/>
    <dl>
      <xsl:apply-templates select="d:indexentry"/>
    </dl>
  </div>
</xsl:template>

<xsl:template match="d:indexdiv/d:title">
  <h3>
    <xsl:apply-templates select="." mode="common.html.attributes"/>
    <xsl:apply-templates/>
  </h3>
</xsl:template>

<!-- ==================================================================== -->

<xsl:template match="d:indexterm">
  <!-- this one must have a name, even if it doesn't have an ID -->
  <xsl:variable name="id">
    <xsl:call-template name="object.id"/>
  </xsl:variable>

  <a class="indexterm" name="{$id}"/>
</xsl:template>

<xsl:template match="d:primary|d:secondary|d:tertiary|d:see|d:seealso">
</xsl:template>

<!-- ==================================================================== -->

<xsl:template match="d:indexentry">
  <xsl:apply-templates select="d:primaryie"/>
</xsl:template>

<xsl:template match="d:primaryie">
  <dt>
    <xsl:apply-templates/>
  </dt>
  <dd>
    <xsl:apply-templates select="following-sibling::d:seeie
                                   [not(preceding-sibling::d:secondaryie)]"
                         mode="indexentry"/>
    <xsl:apply-templates select="following-sibling::d:seealsoie
                                   [not(preceding-sibling::d:secondaryie)]"
                         mode="indexentry"/>
    <xsl:apply-templates select="following-sibling::d:secondaryie"
                         mode="indexentry"/>
  </dd>
</xsl:template>

<!-- Handled in mode to convert flat list to structured output -->
<xsl:template match="d:secondaryie">
</xsl:template>
<xsl:template match="d:tertiaryie">
</xsl:template>
<xsl:template match="d:seeie|d:seealsoie">
</xsl:template>

<xsl:template match="d:secondaryie" mode="indexentry">
  <dl>
    <dt>
      <xsl:apply-templates/>
    </dt>
    <dd>
      <!-- select following see* elements up to next secondaryie or tertiary or end -->
      <xsl:variable name="after.this"
              select="following-sibling::*"/>
      <xsl:variable name="next.entry"
              select="(following-sibling::d:secondaryie|following-sibling::d:tertiaryie)[1]"/>
      <xsl:variable name="before.entry"
                    select="$next.entry/preceding-sibling::*"/>
      <xsl:variable name="see.intersection"
             select="$after.this[count(.|$before.entry) = count($before.entry)]
                                [self::d:seeie or self::d:seealsoie]"/>
      <xsl:choose>
        <xsl:when test="count($see.intersection) != 0">
          <xsl:apply-templates select="$see.intersection" mode="indexentry"/>
        </xsl:when>
        <xsl:when test="count($next.entry) = 0">
          <xsl:apply-templates select="following-sibling::d:seeie"
                               mode="indexentry"/>
          <xsl:apply-templates select="following-sibling::d:seealsoie"
                               mode="indexentry"/>
        </xsl:when>
      </xsl:choose>

      <!-- now process any tertiaryie before the next secondaryie -->
      <xsl:variable name="before.next.secondary" 
              select="following-sibling::d:secondaryie[1]/preceding-sibling::*"/>
      <xsl:variable name="tertiary.intersection"
             select="$after.this[count(.|$before.next.secondary) = 
                                 count($before.next.secondary)]
                                [not(self::d:seeie) and not(self::d:seealsoie)]"/>
      <xsl:choose>
        <xsl:when test="count($tertiary.intersection) != 0">
          <xsl:apply-templates select="$tertiary.intersection"
                               mode="indexentry"/>
        </xsl:when>
        <xsl:when test="not(following-sibling::d:secondaryie)">
          <xsl:apply-templates select="following-sibling::d:tertiaryie"
                               mode="indexentry"/>
        </xsl:when>
      </xsl:choose>
    </dd>
  </dl>
</xsl:template>

<xsl:template match="d:tertiaryie" mode="indexentry">
  <dl>
    <dt>
      <xsl:apply-templates/>
    </dt>
    <dd>
      <!-- select following see* elements up to next secondaryie or tertiary or end -->
      <xsl:variable name="after.this"
              select="following-sibling::*"/>
      <xsl:variable name="next.entry"
              select="(following-sibling::d:secondaryie|following-sibling::d:tertiaryie)[1]"/>
      <xsl:variable name="before.entry"
                    select="$next.entry/preceding-sibling::*"/>
      <xsl:variable name="see.intersection"
             select="$after.this[count(.|$before.entry) = count($before.entry)]
                                [self::d:seeie or self::d:seealsoie]"/>
      <xsl:choose>
        <xsl:when test="count($see.intersection) != 0">
          <xsl:apply-templates select="$see.intersection" mode="indexentry"/>
        </xsl:when>
        <xsl:when test="count($next.entry) = 0">
          <xsl:apply-templates select="following-sibling::d:seeie"
                               mode="indexentry"/>
          <xsl:apply-templates select="following-sibling::d:seealsoie"
                               mode="indexentry"/>
        </xsl:when>
      </xsl:choose>
    </dd>
  </dl>
</xsl:template>

<xsl:template match="d:seeie" mode="indexentry">
  <dt>
    <xsl:text>(</xsl:text>
    <xsl:call-template name="gentext">
      <xsl:with-param name="key" select="'see'"/>
    </xsl:call-template>
    <xsl:text> </xsl:text>
    <xsl:apply-templates/>
    <xsl:text>)</xsl:text>
  </dt>
</xsl:template>

<xsl:template match="d:seealsoie" mode="indexentry">
  <div>
    <xsl:text>(</xsl:text>
    <xsl:call-template name="gentext">
      <xsl:with-param name="key" select="'seealso'"/>
    </xsl:call-template>
    <xsl:text> </xsl:text>
    <xsl:apply-templates/>
    <xsl:text>)</xsl:text>
  </div>
</xsl:template>

</xsl:stylesheet>
