<xsl:stylesheet version="2.0"
    xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
    xmlns="http://maven.apache.org/POM/4.0.0"
    xpath-default-namespace="http://maven.apache.org/POM/4.0.0">

    <xsl:output omit-xml-declaration="yes" />

    <xsl:template match="@* | node()">
        <xsl:copy>
            <xsl:apply-templates select="@* | node()" />
        </xsl:copy>
    </xsl:template>

    <!-- Rewrite artifactIds -->
    <xsl:template match="artifactId[starts-with(text(), 'janusgraph' ) and not(contains(text(), '-jre6')) and not(text() = 'janusgraph-site')]">
        <artifactId xmlns="http://maven.apache.org/POM/4.0.0"><xsl:value-of select="./text()" />-jre6</artifactId>
    </xsl:template>

    <!-- Rewrite compiler target -->
    <xsl:template match="/project/properties/compiler.target">
        <compiler.target>1.6</compiler.target>
    </xsl:template>

    <!-- Rewrite assembly archive names (used in janusgraph-dist) -->
    <xsl:template match="/project/properties/distribution.assembly.name[not(contains(text(), '-jre6'))]">
        <distribution.assembly.name><xsl:value-of select="./text()" />-jre6</distribution.assembly.name>
    </xsl:template>

    <!-- Rewrite failsafe dependency scan configuration (used in janusgraph-dist) -->
    <xsl:template match="dependenciesToScan/dependency[contains(text(), 'org.janusgraph') and not(contains(text(), '-jre6'))]">
        <dependency><xsl:value-of select="./text()" />-jre6</dependency>
    </xsl:template>

</xsl:stylesheet>
