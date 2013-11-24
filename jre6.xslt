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
    <xsl:template match="artifactId[starts-with(text(), 'titan' ) and not(contains(text(), '-jre6')) and not(text() = 'titan-site')]">
        <artifactId xmlns="http://maven.apache.org/POM/4.0.0"><xsl:value-of select="./text()" />-jre6</artifactId>
    </xsl:template>

    <!-- Rewrite compiler target -->
    <xsl:template match="/project/properties/compiler.target">
        <compiler.target>1.6</compiler.target>
    </xsl:template>

    <!-- Rewrite assembly archive names (used in titan-dist) -->
    <xsl:template match="/project/properties/distribution.assembly.name[not(contains(text(), '-jre6'))]">
        <distribution.assembly.name><xsl:value-of select="./text()" />-jre6</distribution.assembly.name>
    </xsl:template>

    <!-- Rewrite failsafe dependency scan configuration (used in titan-dist) -->
    <!-- Persistit is excluded to avoid an overlapping match with the templates below, which would cause a Saxon warning -->
    <xsl:template match="dependenciesToScan/dependency[contains(text(), 'com.thinkaurelius.titan') and not(contains(text(), 'persistit')) and not(contains(text(), '-jre6'))]">
        <dependency><xsl:value-of select="./text()" />-jre6</dependency>
    </xsl:template>

    <!-- Delete persistit dependency and module references -->
    <xsl:template match="dependency[artifactId='titan-persistit']"/>
    <xsl:template match="dependency[artifactId='titan-dist-persistit']"/>
    <xsl:template match="dependenciesToScan/dependency[contains(text(), 'persistit')]"/>
    <xsl:template match="modules/module[contains(text(), 'persistit')]"/>

</xsl:stylesheet>
