<?xml version="1.0"?>
<xsl:stylesheet exclude-result-prefixes="d"
                 xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
                xmlns:d="https://docbook.org/ns/docbook"
version="1.0">

<!-- ********************************************************************
     $Id: autoidx-ng.xsl 6910 2007-06-28 23:23:30Z xmldoc $
     ********************************************************************

     This file is part of the DocBook XSL Stylesheet distribution.
     See ../README or http://docbook.sf.net/ for copyright
     copyright and other information.

     ******************************************************************** -->

<!-- You should have this directly in your customization file. -->
<!-- This file is there only to retain backward compatibility. -->
<xsl:import href="autoidx-kosek.xsl"/>
<xsl:param name="index.method">kosek</xsl:param>

</xsl:stylesheet>
