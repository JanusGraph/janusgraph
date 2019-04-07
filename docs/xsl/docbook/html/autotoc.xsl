<?xml version='1.0'?>
<xsl:stylesheet exclude-result-prefixes="d"
                 xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
                xmlns:d="https://docbook.org/ns/docbook"
version='1.0'>

<!-- ********************************************************************
     $Id: autotoc.xsl 9692 2012-12-16 02:31:34Z dcramer $
     ********************************************************************

     This file is part of the XSL DocBook Stylesheet distribution.
     See ../README or http://docbook.sf.net/release/xsl/current/ for
     copyright and other information.

     ******************************************************************** -->

<xsl:variable name="toc.listitem.type">
  <xsl:choose>
    <xsl:when test="$toc.list.type = 'dl'">dt</xsl:when>
    <xsl:otherwise>li</xsl:otherwise>
  </xsl:choose>
</xsl:variable>

<!-- this is just hack because dl and ul aren't completely isomorphic -->
<xsl:variable name="toc.dd.type">
  <xsl:choose>
    <xsl:when test="$toc.list.type = 'dl'">dd</xsl:when>
    <xsl:otherwise></xsl:otherwise>
  </xsl:choose>
</xsl:variable>

<xsl:template name="make.toc">
  <xsl:param name="toc-context" select="."/>
  <xsl:param name="toc.title.p" select="true()"/>
  <xsl:param name="nodes" select="/NOT-AN-ELEMENT"/>

  <xsl:variable name="nodes.plus" select="$nodes | d:qandaset"/>

  <xsl:variable name="toc.title">
    <xsl:if test="$toc.title.p">
      <xsl:choose>
        <xsl:when test="$make.clean.html != 0">
          <div class="toc-title">
            <xsl:call-template name="gentext">
              <xsl:with-param name="key">TableofContents</xsl:with-param>
            </xsl:call-template>
          </div>
        </xsl:when>
        <xsl:otherwise>
          <p>
            <b>
              <xsl:call-template name="gentext">
                <xsl:with-param name="key">TableofContents</xsl:with-param>
              </xsl:call-template>
            </b>
          </p>
        </xsl:otherwise>
      </xsl:choose>
    </xsl:if>
  </xsl:variable>

  <xsl:choose>
    <xsl:when test="$manual.toc != ''">
      <xsl:variable name="id">
        <xsl:call-template name="object.id"/>
      </xsl:variable>
      <xsl:variable name="toc" select="document($manual.toc, .)"/>
      <xsl:variable name="tocentry" select="$toc//d:tocentry[@linkend=$id]"/>
      <xsl:if test="$tocentry and $tocentry/*">
        <div class="toc">
          <xsl:copy-of select="$toc.title"/>
          <xsl:element name="{$toc.list.type}">
            <xsl:call-template name="toc.list.attributes">
              <xsl:with-param name="toc-context" select="$toc-context"/>
              <xsl:with-param name="toc.title.p" select="$toc.title.p"/>
              <xsl:with-param name="nodes" select="$nodes"/>
            </xsl:call-template>
            <xsl:call-template name="manual-toc">
              <xsl:with-param name="tocentry" select="$tocentry/*[1]"/>
            </xsl:call-template>
          </xsl:element>
        </div>
      </xsl:if>
    </xsl:when>
    <xsl:otherwise>
      <xsl:choose>
        <xsl:when test="$qanda.in.toc != 0">
          <xsl:if test="$nodes.plus">
            <div class="toc">
              <xsl:copy-of select="$toc.title"/>
              <xsl:element name="{$toc.list.type}">
                <xsl:call-template name="toc.list.attributes">
                  <xsl:with-param name="toc-context" select="$toc-context"/>
                  <xsl:with-param name="toc.title.p" select="$toc.title.p"/>
                  <xsl:with-param name="nodes" select="$nodes"/>
                </xsl:call-template>
                <xsl:apply-templates select="$nodes.plus" mode="toc">
                  <xsl:with-param name="toc-context" select="$toc-context"/>
                </xsl:apply-templates>
              </xsl:element>
            </div>
          </xsl:if>
        </xsl:when>
        <xsl:otherwise>
          <xsl:if test="$nodes">
            <div class="toc">
              <xsl:copy-of select="$toc.title"/>
              <xsl:element name="{$toc.list.type}">
                <xsl:call-template name="toc.list.attributes">
                  <xsl:with-param name="toc-context" select="$toc-context"/>
                  <xsl:with-param name="toc.title.p" select="$toc.title.p"/>
                  <xsl:with-param name="nodes" select="$nodes"/>
                </xsl:call-template>
                <xsl:apply-templates select="$nodes" mode="toc">
                  <xsl:with-param name="toc-context" select="$toc-context"/>
                </xsl:apply-templates>
              </xsl:element>
            </div>
          </xsl:if>
        </xsl:otherwise>
      </xsl:choose>

    </xsl:otherwise>
  </xsl:choose>
</xsl:template>

<xsl:template name="toc.list.attributes">
  <xsl:param name="toc-context" select="."/>
  <xsl:param name="toc.title.p" select="true()"/>
  <xsl:param name="nodes" select="/NOT-AN-ELEMENT"/>

  <xsl:attribute name="class">toc</xsl:attribute>
</xsl:template>

<xsl:template name="make.lots">
  <xsl:param name="toc.params" select="''"/>
  <xsl:param name="toc"/>

  <xsl:if test="contains($toc.params, 'toc')">
    <xsl:copy-of select="$toc"/>
  </xsl:if>

  <xsl:if test="contains($toc.params, 'figure')">
    <xsl:call-template name="list.of.titles">
      <xsl:with-param name="titles" select="'figure'"/>
      <xsl:with-param name="nodes" select=".//d:figure"/>
    </xsl:call-template>
  </xsl:if>

  <xsl:if test="contains($toc.params, 'table')">
    <xsl:call-template name="list.of.titles">
      <xsl:with-param name="titles" select="'table'"/>
      <xsl:with-param name="nodes" select=".//d:table[not(@tocentry = 0)]"/>
    </xsl:call-template>
  </xsl:if>

  <xsl:if test="contains($toc.params, 'example')">
    <xsl:call-template name="list.of.titles">
      <xsl:with-param name="titles" select="'example'"/>
      <xsl:with-param name="nodes" select=".//d:example"/>
    </xsl:call-template>
  </xsl:if>

  <xsl:if test="contains($toc.params, 'equation')">
    <xsl:call-template name="list.of.titles">
      <xsl:with-param name="titles" select="'equation'"/>
      <xsl:with-param name="nodes" select=".//d:equation[d:title or d:info/d:title]"/>
    </xsl:call-template>
  </xsl:if>

  <xsl:if test="contains($toc.params, 'procedure')">
    <xsl:call-template name="list.of.titles">
      <xsl:with-param name="titles" select="'procedure'"/>
      <xsl:with-param name="nodes" select=".//d:procedure[d:title]"/>
    </xsl:call-template>
  </xsl:if>
</xsl:template>

<!-- ====================================================================== -->

<xsl:template name="set.toc">
  <xsl:param name="toc-context" select="."/>
  <xsl:param name="toc.title.p" select="true()"/>

  <xsl:call-template name="make.toc">
    <xsl:with-param name="toc-context" select="$toc-context"/>
    <xsl:with-param name="toc.title.p" select="$toc.title.p"/>
    <xsl:with-param name="nodes" select="d:book|d:setindex|d:set"/>
  </xsl:call-template>
</xsl:template>

<xsl:template name="division.toc">
  <xsl:param name="toc-context" select="."/>
  <xsl:param name="toc.title.p" select="true()"/>

  <xsl:call-template name="make.toc">
    <xsl:with-param name="toc-context" select="$toc-context"/>
    <xsl:with-param name="toc.title.p" select="$toc.title.p"/>
    <xsl:with-param name="nodes" select="d:part|d:reference
                                         |d:preface|d:chapter|d:appendix
                                         |d:article
                                         |d:topic
                                         |d:bibliography|d:glossary|d:index
                                         |d:refentry
                                         |d:bridgehead[$bridgehead.in.toc != 0]"/>

  </xsl:call-template>
</xsl:template>

<xsl:template name="component.toc">
  <xsl:param name="toc-context" select="."/>
  <xsl:param name="toc.title.p" select="true()"/>

  <xsl:call-template name="make.toc">
    <xsl:with-param name="toc-context" select="$toc-context"/>
    <xsl:with-param name="toc.title.p" select="$toc.title.p"/>
    <xsl:with-param name="nodes" select="d:section|d:sect1
                                         |d:simplesect[$simplesect.in.toc != 0]
                                         |d:topic
                                         |d:refentry
                                         |d:article|d:bibliography|d:glossary
                                         |d:appendix|d:index
                                         |d:bridgehead[not(@renderas)
                                                     and $bridgehead.in.toc != 0]
                                         |.//d:bridgehead[@renderas='sect1'
                                                        and $bridgehead.in.toc != 0]"/>
  </xsl:call-template>
</xsl:template>

<xsl:template name="component.toc.separator">
  <!-- Customize to output something between
       component.toc and first output -->
</xsl:template>

<xsl:template name="section.toc">
  <xsl:param name="toc-context" select="."/>
  <xsl:param name="toc.title.p" select="true()"/>

  <xsl:call-template name="make.toc">
    <xsl:with-param name="toc-context" select="$toc-context"/>
    <xsl:with-param name="toc.title.p" select="$toc.title.p"/>
    <xsl:with-param name="nodes"
                    select="d:section|d:sect1|d:sect2|d:sect3|d:sect4|d:sect5|d:refentry
                           |d:bridgehead[$bridgehead.in.toc != 0]"/>

  </xsl:call-template>
</xsl:template>

<xsl:template name="section.toc.separator">
  <!-- Customize to output something between
       section.toc and first output -->
</xsl:template>
<!-- ==================================================================== -->

<xsl:template name="subtoc">
  <xsl:param name="toc-context" select="."/>
  <xsl:param name="nodes" select="NOT-AN-ELEMENT"/>

  <xsl:variable name="nodes.plus" select="$nodes | d:qandaset"/>

  <xsl:variable name="subtoc">
    <xsl:element name="{$toc.list.type}">
      <xsl:choose>
        <xsl:when test="$qanda.in.toc != 0">
          <xsl:apply-templates mode="toc" select="$nodes.plus">
            <xsl:with-param name="toc-context" select="$toc-context"/>
          </xsl:apply-templates>
        </xsl:when>
        <xsl:otherwise>
          <xsl:apply-templates mode="toc" select="$nodes">
            <xsl:with-param name="toc-context" select="$toc-context"/>
          </xsl:apply-templates>
        </xsl:otherwise>
      </xsl:choose>
    </xsl:element>
  </xsl:variable>

  <xsl:variable name="depth">
    <xsl:choose>
      <xsl:when test="local-name(.) = 'section'">
        <xsl:value-of select="count(ancestor::d:section) + 1"/>
      </xsl:when>
      <xsl:when test="local-name(.) = 'sect1'">1</xsl:when>
      <xsl:when test="local-name(.) = 'sect2'">2</xsl:when>
      <xsl:when test="local-name(.) = 'sect3'">3</xsl:when>
      <xsl:when test="local-name(.) = 'sect4'">4</xsl:when>
      <xsl:when test="local-name(.) = 'sect5'">5</xsl:when>
      <xsl:when test="local-name(.) = 'refsect1'">1</xsl:when>
      <xsl:when test="local-name(.) = 'refsect2'">2</xsl:when>
      <xsl:when test="local-name(.) = 'refsect3'">3</xsl:when>
      <xsl:when test="local-name(.) = 'topic'">1</xsl:when>
      <xsl:when test="local-name(.) = 'simplesect'">
        <!-- sigh... -->
        <xsl:choose>
          <xsl:when test="local-name(..) = 'section'">
            <xsl:value-of select="count(ancestor::d:section)"/>
          </xsl:when>
          <xsl:when test="local-name(..) = 'sect1'">2</xsl:when>
          <xsl:when test="local-name(..) = 'sect2'">3</xsl:when>
          <xsl:when test="local-name(..) = 'sect3'">4</xsl:when>
          <xsl:when test="local-name(..) = 'sect4'">5</xsl:when>
          <xsl:when test="local-name(..) = 'sect5'">6</xsl:when>
          <xsl:when test="local-name(..) = 'topic'">2</xsl:when>
          <xsl:when test="local-name(..) = 'refsect1'">2</xsl:when>
          <xsl:when test="local-name(..) = 'refsect2'">3</xsl:when>
          <xsl:when test="local-name(..) = 'refsect3'">4</xsl:when>
          <xsl:otherwise>1</xsl:otherwise>
        </xsl:choose>
      </xsl:when>
      <xsl:otherwise>0</xsl:otherwise>
    </xsl:choose>
  </xsl:variable>

  <xsl:variable name="depth.from.context" select="count(ancestor::*)-count($toc-context/ancestor::*)"/>

  <xsl:variable name="subtoc.list">
    <xsl:choose>
      <xsl:when test="$toc.dd.type = ''">
        <xsl:copy-of select="$subtoc"/>
      </xsl:when>
      <xsl:otherwise>
        <xsl:element name="{$toc.dd.type}">
          <xsl:copy-of select="$subtoc"/>
        </xsl:element>
      </xsl:otherwise>
    </xsl:choose>
  </xsl:variable>

  <xsl:element name="{$toc.listitem.type}">
    <xsl:call-template name="toc.line">
      <xsl:with-param name="toc-context" select="$toc-context"/>
    </xsl:call-template>
    <xsl:if test="$toc.listitem.type = 'li' and
                  ( (self::d:set or self::d:book or self::d:part) or 
                        $toc.section.depth > $depth) and 
                  ( ($qanda.in.toc = 0 and count($nodes)&gt;0) or
                    ($qanda.in.toc != 0 and count($nodes.plus)&gt;0) )
                  and $toc.max.depth > $depth.from.context">
      <xsl:copy-of select="$subtoc.list"/>
    </xsl:if>
  </xsl:element>
  <xsl:if test="$toc.listitem.type != 'li' and
                  ( (self::d:set or self::d:book or self::d:part) or 
                        $toc.section.depth > $depth) and 
                ( ($qanda.in.toc = 0 and count($nodes)&gt;0) or
                  ($qanda.in.toc != 0 and count($nodes.plus)&gt;0) )
                and $toc.max.depth > $depth.from.context">
    <xsl:copy-of select="$subtoc.list"/>
  </xsl:if>
</xsl:template>

<xsl:template name="toc.line">
  <xsl:param name="toc-context" select="."/>
  <xsl:param name="depth" select="1"/>
  <xsl:param name="depth.from.context" select="8"/>

 <span>
  <xsl:attribute name="class"><xsl:value-of select="local-name(.)"/></xsl:attribute>

  <!-- * if $autotoc.label.in.hyperlink is zero, then output the label -->
  <!-- * before the hyperlinked title (as the DSSSL stylesheet does) -->
  <xsl:if test="$autotoc.label.in.hyperlink = 0">
    <xsl:variable name="label">
      <xsl:apply-templates select="." mode="label.markup"/>
    </xsl:variable>
    <xsl:copy-of select="$label"/>
    <xsl:if test="$label != ''">
      <xsl:value-of select="$autotoc.label.separator"/>
    </xsl:if>
  </xsl:if>

  <a>
    <xsl:attribute name="href">
      <xsl:call-template name="href.target">
        <xsl:with-param name="context" select="$toc-context"/>
        <xsl:with-param name="toc-context" select="$toc-context"/>
      </xsl:call-template>
    </xsl:attribute>
    
  <!-- * if $autotoc.label.in.hyperlink is non-zero, then output the label -->
  <!-- * as part of the hyperlinked title -->
  <xsl:if test="not($autotoc.label.in.hyperlink = 0)">
    <xsl:variable name="label">
      <xsl:apply-templates select="." mode="label.markup"/>
    </xsl:variable>
    <xsl:copy-of select="$label"/>
    <xsl:if test="$label != ''">
      <xsl:value-of select="$autotoc.label.separator"/>
    </xsl:if>
  </xsl:if>

    <xsl:apply-templates select="." mode="titleabbrev.markup"/>
  </a>
  </span>
</xsl:template>

<xsl:template match="d:book" mode="toc">
  <xsl:param name="toc-context" select="."/>

  <xsl:call-template name="subtoc">
    <xsl:with-param name="toc-context" select="$toc-context"/>
    <xsl:with-param name="nodes" select="d:part|d:reference
                                         |d:preface|d:chapter|d:appendix
                                         |d:article
                                         |d:topic
                                         |d:bibliography|d:glossary|d:index
                                         |d:refentry
                                         |d:bridgehead[$bridgehead.in.toc != 0]"/>
  </xsl:call-template>
</xsl:template>

<xsl:template match="d:setindex" mode="toc">
  <xsl:param name="toc-context" select="."/>

  <!-- If the setindex tag is not empty, it should be it in the TOC -->
  <xsl:if test="* or $generate.index != 0">
    <xsl:call-template name="subtoc">
      <xsl:with-param name="toc-context" select="$toc-context"/>
    </xsl:call-template>
  </xsl:if>
</xsl:template>

<xsl:template match="d:part|d:reference" mode="toc">
  <xsl:param name="toc-context" select="."/>

  <xsl:call-template name="subtoc">
    <xsl:with-param name="toc-context" select="$toc-context"/>
    <xsl:with-param name="nodes" select="d:appendix|d:chapter|d:article|d:topic
                                         |d:index|d:glossary|d:bibliography
                                         |d:preface|d:reference|d:refentry
                                         |d:bridgehead[$bridgehead.in.toc != 0]"/>
  </xsl:call-template>
</xsl:template>

<xsl:template match="d:preface|d:chapter|d:appendix|d:article|d:topic" mode="toc">
  <xsl:param name="toc-context" select="."/>

  <xsl:call-template name="subtoc">
    <xsl:with-param name="toc-context" select="$toc-context"/>
    <xsl:with-param name="nodes" select="d:section|d:sect1
                                         |d:simplesect[$simplesect.in.toc != 0]
                                         |d:topic
                                         |d:refentry
                                         |d:glossary|d:bibliography|d:index
                                         |d:bridgehead[$bridgehead.in.toc != 0]"/>
  </xsl:call-template>
</xsl:template>

<xsl:template match="d:sect1" mode="toc">
  <xsl:param name="toc-context" select="."/>
  <xsl:call-template name="subtoc">
    <xsl:with-param name="toc-context" select="$toc-context"/>
    <xsl:with-param name="nodes" select="d:sect2
                                         |d:bridgehead[$bridgehead.in.toc != 0]"/>
  </xsl:call-template>
</xsl:template>

<xsl:template match="d:sect2" mode="toc">
  <xsl:param name="toc-context" select="."/>

  <xsl:call-template name="subtoc">
    <xsl:with-param name="toc-context" select="$toc-context"/>
    <xsl:with-param name="nodes" select="d:sect3
                                         |d:bridgehead[$bridgehead.in.toc != 0]"/>
  </xsl:call-template>
</xsl:template>

<xsl:template match="d:sect3" mode="toc">
  <xsl:param name="toc-context" select="."/>

  <xsl:call-template name="subtoc">
    <xsl:with-param name="toc-context" select="$toc-context"/>
    <xsl:with-param name="nodes" select="d:sect4
                                         |d:bridgehead[$bridgehead.in.toc != 0]"/>
  </xsl:call-template>
</xsl:template>

<xsl:template match="d:sect4" mode="toc">
  <xsl:param name="toc-context" select="."/>

  <xsl:call-template name="subtoc">
    <xsl:with-param name="toc-context" select="$toc-context"/>
    <xsl:with-param name="nodes" select="d:sect5
                                         |d:bridgehead[$bridgehead.in.toc != 0]"/>
  </xsl:call-template>
</xsl:template>

<xsl:template match="d:sect5" mode="toc">
  <xsl:param name="toc-context" select="."/>

  <xsl:call-template name="subtoc">
    <xsl:with-param name="toc-context" select="$toc-context"/>
  </xsl:call-template>
</xsl:template>

<xsl:template match="d:simplesect" mode="toc">
  <xsl:param name="toc-context" select="."/>

  <xsl:call-template name="subtoc">
    <xsl:with-param name="toc-context" select="$toc-context"/>
  </xsl:call-template>
</xsl:template>

<xsl:template match="d:section" mode="toc">
  <xsl:param name="toc-context" select="."/>

  <xsl:call-template name="subtoc">
    <xsl:with-param name="toc-context" select="$toc-context"/>
    <xsl:with-param name="nodes" select="d:section|d:refentry
                                         |d:simplesect[$simplesect.in.toc != 0]
                                         |d:bridgehead[$bridgehead.in.toc != 0]"/>
  </xsl:call-template>
</xsl:template>

<xsl:template match="d:topic" mode="toc">
  <xsl:param name="toc-context" select="."/>

  <xsl:call-template name="subtoc">
    <xsl:with-param name="toc-context" select="$toc-context"/>
    <xsl:with-param name="nodes" select="d:section|d:refentry
                                         |d:simplesect[$simplesect.in.toc != 0]
                                         |d:bridgehead[$bridgehead.in.toc != 0]"/>
  </xsl:call-template>
</xsl:template>

<xsl:template match="d:bridgehead" mode="toc">
  <xsl:param name="toc-context" select="."/>

  <xsl:if test="$bridgehead.in.toc != 0">
    <xsl:call-template name="subtoc">
      <xsl:with-param name="toc-context" select="$toc-context"/>
    </xsl:call-template>
  </xsl:if>
</xsl:template>

<xsl:template match="d:bibliography|d:glossary" mode="toc">
  <xsl:param name="toc-context" select="."/>

  <xsl:call-template name="subtoc">
    <xsl:with-param name="toc-context" select="$toc-context"/>
  </xsl:call-template>
</xsl:template>

<xsl:template match="d:index" mode="toc">
  <xsl:param name="toc-context" select="."/>

  <!-- If the index tag is not empty, it should be it in the TOC -->
  <xsl:if test="* or $generate.index != 0">
    <xsl:call-template name="subtoc">
      <xsl:with-param name="toc-context" select="$toc-context"/>
    </xsl:call-template>
  </xsl:if>
</xsl:template>

<xsl:template match="d:refentry" mode="toc">
  <xsl:param name="toc-context" select="."/>

  <xsl:variable name="refmeta" select=".//d:refmeta"/>
  <xsl:variable name="refentrytitle" select="$refmeta//d:refentrytitle"/>
  <xsl:variable name="refnamediv" select=".//d:refnamediv"/>
  <xsl:variable name="refname" select="$refnamediv//d:refname"/>
  <xsl:variable name="refdesc" select="$refnamediv//d:refdescriptor"/>
  <xsl:variable name="title">
    <xsl:choose>
      <xsl:when test="$refentrytitle">
        <xsl:apply-templates select="$refentrytitle[1]"
			     mode="titleabbrev.markup"/>
      </xsl:when>
      <xsl:when test="$refdesc">
        <xsl:apply-templates select="$refdesc"
			     mode="titleabbrev.markup"/>
      </xsl:when>
      <xsl:when test="$refname">
        <xsl:apply-templates select="$refname[1]"
			     mode="titleabbrev.markup"/>
      </xsl:when>
    </xsl:choose>
  </xsl:variable>

  <xsl:element name="{$toc.listitem.type}">
    <span class='refentrytitle'>
      <a>
        <xsl:attribute name="href">
          <xsl:call-template name="href.target">
            <xsl:with-param name="toc-context" select="$toc-context"/>
          </xsl:call-template>
        </xsl:attribute>
        <xsl:copy-of select="$title"/>
      </a>
    </span>
    <span class='refpurpose'>
      <xsl:if test="$annotate.toc != 0">
        <!-- * DocBook 5 says inlinemediaobject (among other things) -->
        <!-- * is allowed in refpurpose; so we need to run -->
        <!-- * apply-templates on refpurpose here, instead of value-of  -->
        <xsl:apply-templates select="d:refnamediv/d:refpurpose" mode="no.anchor.mode"/>
      </xsl:if>
    </span>
  </xsl:element>
</xsl:template>

<xsl:template match="d:title" mode="toc">
  <xsl:param name="toc-context" select="."/>

  <a>
    <xsl:attribute name="href">
      <xsl:call-template name="href.target">
        <xsl:with-param name="object" select=".."/>
        <xsl:with-param name="toc-context" select="$toc-context"/>
      </xsl:call-template>
    </xsl:attribute>
    <xsl:apply-templates/>
  </a>
</xsl:template>

<xsl:template name="manual-toc">
  <xsl:param name="toc-context" select="."/>
  <xsl:param name="tocentry"/>
  <xsl:param name="toc.title.p" select="true()"/>
  <xsl:param name="nodes" select="/NOT-AN-ELEMENT"/>

  <!-- be careful, we don't want to change the current document to the other tree! -->

  <xsl:if test="$tocentry">
    <xsl:variable name="node" select="key('id', $tocentry/@linkend)"/>

    <xsl:element name="{$toc.listitem.type}">
      <xsl:variable name="label">
        <xsl:apply-templates select="$node" mode="label.markup"/>
      </xsl:variable>
      <xsl:copy-of select="$label"/>
      <xsl:if test="$label != ''">
        <xsl:value-of select="$autotoc.label.separator"/>
      </xsl:if>
      <a>
        <xsl:attribute name="href">
          <xsl:call-template name="href.target">
            <xsl:with-param name="object" select="$node"/>
            <xsl:with-param name="toc-context" select="$toc-context"/>
          </xsl:call-template>
        </xsl:attribute>
        <xsl:apply-templates select="$node" mode="titleabbrev.markup"/>
      </a>
    </xsl:element>

    <xsl:if test="$tocentry/*">
      <xsl:element name="{$toc.list.type}">
        <xsl:call-template name="toc.list.attributes">
          <xsl:with-param name="toc-context" select="$toc-context"/>
          <xsl:with-param name="toc.title.p" select="$toc.title.p"/>
          <xsl:with-param name="nodes" select="$nodes"/>
        </xsl:call-template>
        <xsl:call-template name="manual-toc">
          <xsl:with-param name="tocentry" select="$tocentry/*[1]"/>
        </xsl:call-template>
      </xsl:element>
    </xsl:if>

    <xsl:if test="$tocentry/following-sibling::*">
      <xsl:call-template name="manual-toc">
        <xsl:with-param name="tocentry" select="$tocentry/following-sibling::*[1]"/>
      </xsl:call-template>
    </xsl:if>
  </xsl:if>
</xsl:template>

<!-- ==================================================================== -->

<xsl:template name="list.of.titles">
  <xsl:param name="toc-context" select="."/>
  <xsl:param name="titles" select="'table'"/>
  <xsl:param name="nodes" select=".//d:table"/>

  <xsl:if test="$nodes">
    <div class="list-of-{$titles}s">
      <xsl:choose>
        <xsl:when test="$make.clean.html != 0">
          <div class="toc-title">
            <xsl:call-template name="gentext">
              <xsl:with-param name="key">
                <xsl:choose>
                  <xsl:when test="$titles='table'">ListofTables</xsl:when>
                  <xsl:when test="$titles='figure'">ListofFigures</xsl:when>
                  <xsl:when test="$titles='equation'">ListofEquations</xsl:when>
                  <xsl:when test="$titles='example'">ListofExamples</xsl:when>
                  <xsl:when test="$titles='procedure'">ListofProcedures</xsl:when>
                  <xsl:otherwise>ListofUnknown</xsl:otherwise>
                </xsl:choose>
              </xsl:with-param>
            </xsl:call-template>
          </div>
        </xsl:when>
        <xsl:otherwise>
          <p>
            <b>
              <xsl:call-template name="gentext">
                <xsl:with-param name="key">
                  <xsl:choose>
                    <xsl:when test="$titles='table'">ListofTables</xsl:when>
                    <xsl:when test="$titles='figure'">ListofFigures</xsl:when>
                    <xsl:when test="$titles='equation'">ListofEquations</xsl:when>
                    <xsl:when test="$titles='example'">ListofExamples</xsl:when>
                    <xsl:when test="$titles='procedure'">ListofProcedures</xsl:when>
                    <xsl:otherwise>ListofUnknown</xsl:otherwise>
                  </xsl:choose>
                </xsl:with-param>
              </xsl:call-template>
            </b>
          </p>
        </xsl:otherwise>
      </xsl:choose>

      <xsl:element name="{$toc.list.type}">
        <xsl:apply-templates select="$nodes" mode="toc">
          <xsl:with-param name="toc-context" select="$toc-context"/>
        </xsl:apply-templates>
      </xsl:element>
    </div>
  </xsl:if>
</xsl:template>

<xsl:template match="d:figure|d:table|d:example|d:equation|d:procedure" mode="toc">
  <xsl:param name="toc-context" select="."/>

  <xsl:element name="{$toc.listitem.type}">
    <xsl:variable name="label">
      <xsl:apply-templates select="." mode="label.markup"/>
    </xsl:variable>
    <xsl:copy-of select="$label"/>
    <xsl:if test="$label != ''">
      <xsl:value-of select="$autotoc.label.separator"/>
    </xsl:if>
    <a>
      <xsl:attribute name="href">
        <xsl:call-template name="href.target">
          <xsl:with-param name="toc-context" select="$toc-context"/>
        </xsl:call-template>
      </xsl:attribute>
      <xsl:apply-templates select="." mode="titleabbrev.markup"/>
    </a>
  </xsl:element>
</xsl:template>

<!-- Used only if qanda.in.toc parameter is non-zero -->
<xsl:template match="d:qandaset" mode="toc">
  <xsl:param name="toc-context" select="."/>
  <xsl:call-template name="subtoc">
    <xsl:with-param name="toc-context" select="$toc-context"/>
    <xsl:with-param name="nodes" select="d:qandadiv | d:qandaentry"/>
  </xsl:call-template>
</xsl:template>

<xsl:template match="d:qandadiv|d:qandaentry" mode="toc">
  <xsl:apply-templates select="." mode="qandatoc.mode"/>
</xsl:template>

</xsl:stylesheet>

