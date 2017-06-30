<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
                xmlns:d="http://docbook.org/ns/docbook"
                version="1.0">

  <!-- Navigation and section labels -->
  <xsl:param name="suppress.navigation" select="0"/>
  <xsl:param name="suppress.header.navigation" select="0"/>
  <xsl:param name="suppress.footer.navigation" select="0"/>
  <xsl:param name="navig.showtitles" select="1"/>
  <xsl:param name="section.autolabel.max.depth" select="5"/>
  <xsl:param name="section.autolabel" select="1"/>
  <xsl:param name="section.label.includes.component.label" select="1"/>

  <!-- Chunking -->
  <xsl:param name="chunk.first.sections" select="1"/>
  <xsl:param name="chunk.section.depth" select="0"/>
  <xsl:param name="chunk.toc" select="''"/>
  <xsl:param name="chunk.tocs.and.lots" select="0"/>
  <xsl:param name="use.id.as.filename" select="1"/>

  <!-- TOCs -->
  <!-- For the distinction between toc.section.depth and
       toc.max.depth, see:
       http://www.sagehill.net/docbookxsl/TOCcontrol.html#TOClevels
       -->
  <!-- <xsl:param name="toc.section.depth" select="0"/> -->
  <xsl:param name="toc.max.depth" select="2"/>

  <!-- Code syntax highlighting -->
  <xsl:param name="highlight.source" select="1"/>

  <!-- Table configuration -->
  <xsl:param name="table.borders.with.css" select="1"/>
  <xsl:param name="table.cell.border.color" select="'#747474'"/>
  <xsl:param name="table.cell.border.style" select="'solid'"/>
  <xsl:param name="table.cell.border.thickness" select="'1px'"/>
  <xsl:param name="table.frame.border.color" select="'#1b8669'"/>
  <xsl:param name="table.frame.border.style" select="'solid'"/>
  <xsl:param name="table.frame.border.thickness" select="'1px'"/>
  <xsl:param name="tablecolumns.extension" select="'1'"/>
  <xsl:param name="html.cellpadding" select="'4px'"/>
  <xsl:param name="html.cellspacing" select="''"/>

  <xsl:param name="admon.graphics" select="1"/>
  <xsl:param name="admon.graphics.path">images/icons/</xsl:param>

  <!-- Misc style -->
  <xsl:param name="phrase.propagates.style" select="1"/>

  <xsl:param name="janusgraph.top.nav.links" select="0" />

  <!-- JanusGraph-themed output templates -->
  <xsl:template name="janusgraph.head">
    <script type='text/javascript' src='js/jquery/jquery-1.11.0.js'></script>
    <script type='text/javascript' src='js/jquery/jquery-migrate-1.2.1.min.js'></script>

    <link rel='stylesheet' id='inline-blob-janusgraph-docs-specific' href='css/docs.css' type='text/css' media='all' />
    <link rel='apple-touch-icon' type='image/png' href='images/janusgraph-logomark.png' />

    <script type="text/javascript">
      WebFontConfig = {
        google: {
          families: [
            "Lato:400,400italic,700,700italic:latin,greek-ext,cyrillic,latin-ext,greek,cyrillic-ext,vietnamese",
            "Open+Sans:400,400italic,700,700italic:latin,greek-ext,cyrillic,latin-ext,greek,cyrillic-ext,vietnamese",
            "Antic+Slab:400,400italic,700,700italic:latin,greek-ext,cyrillic,latin-ext,greek,cyrillic-ext,vietnamese"
          ]
        }
      };
      (function() {
      var wf = document.createElement('script');
      wf.src = ('https:' == document.location.protocol ? 'https' : 'http') +
        '://ajax.googleapis.com/ajax/libs/webfont/1/webfont.js';
	wf.type = 'text/javascript';
	wf.async = 'true';
	var s = document.getElementsByTagName('script')[0];
	s.parentNode.insertBefore(wf, s);
	})();
    </script>
  </xsl:template>

  <xsl:template name="janusgraph.header.navigation">
    <xsl:param name="prev" select="/d:foo"/>
    <xsl:param name="next" select="/d:foo"/>
    <xsl:param name="nav.context"/>
    <div class="header-wrapper">
      <header id="header">
        <ul class="header-list">
          <li class="header-item"><a href="http://janusgraph.org"><img src="images/janusgraph-logo.png" alt="JanusGraph" class="normal_logo" /></a></li>
          <li class="header-item-right"><a href="https://github.com/JanusGraph/janusgraph/releases">Download JanusGraph</a></li>
          <li class="header-item-right"><a href="https://github.com/JanusGraph/janusgraph/releases">Other Versions</a></li>
          <li class="header-item-right"><a href="index.html">Documentation ($MAVEN{project.version})</a></li>
        </ul>
      </header>
    </div>
  </xsl:template>

  <xsl:template name="janusgraph.body">
    <xsl:param name="prev"/>
    <xsl:param name="next"/>
    <xsl:param name="maincontent"/>
    <xsl:param name="headercontent"/>
    <xsl:param name="footercontent"/>
    <body>
      <xsl:call-template name="body.attributes"/>
      <div id="wrapper" >

	<xsl:copy-of select="$headercontent"/>

	<div id="main" class="clearfix width-100">
          <xsl:call-template name="breadcrumbs"/>
          <xsl:copy-of select="$maincontent"/>
	</div>
	<div class="clearer"></div>

	<xsl:copy-of select="$footercontent"/>

        <div class="footer-wrapper">
          <footer id="footer">
            <div class="copyright">
              Copyright © 2017 JanusGraph Authors. All rights reserved.<br />
              The Linux Foundation has registered trademarks and uses trademarks. For a list of<br />
              trademarks of The Linux Foundation, please see our <a href="https://www.linuxfoundation.org/trademark-usage">Trademark Usage</a> page.<br />
              Cassandra, Groovy, HBase, Hadoop, Lucene, Solr, and TinkerPop are trademarks of the Apache Software Foundation.<br />
              Berkeley DB and Berkeley DB Java Edition are trademarks of Oracle.<br />
              Documentation generated with <a href="http://www.methods.co.nz/asciidoc/">AsciiDoc</a>, <a href="http://asciidoctor.org/">AsciiDoctor</a>, <a href="http://docbook.sourceforge.net/">DocBook</a>, and <a href="http://saxon.sourceforge.net/">Saxon</a>.
        	  </div>
        	</footer>
        </div>
      </div>
    </body>
  </xsl:template>

  <!-- From the DocBook XSL guide
       http://www.sagehill.net/docbookxsl/HTMLHeaders.html -->
  <xsl:template name="breadcrumbs">
    <xsl:param name="this.node" select="."/>
    <xsl:if test="0 != count($this.node/ancestor::*)">
      <div class="breadcrumbs">
        <xsl:for-each select="$this.node/ancestor::*">
          <span class="breadcrumb-link">
            <a>
              <xsl:attribute name="href">
                <xsl:call-template name="href.target">
                  <xsl:with-param name="object" select="."/>
                  <xsl:with-param name="context" select="$this.node"/>
                </xsl:call-template>
              </xsl:attribute>
              <xsl:apply-templates select="." mode="title.markup"/>
            </a>
          </span>
          <xsl:text> &gt; </xsl:text>
        </xsl:for-each>
        <!-- And display the current node, but not as a link -->
        <span class="breadcrumb-node">
          <xsl:apply-templates select="$this.node" mode="title.markup"/>
        </span>
      </div>
    </xsl:if>
  </xsl:template>

</xsl:stylesheet>
