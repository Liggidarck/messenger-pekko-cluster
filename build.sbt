ThisBuild / scalaVersion := "2.13.18"
ThisBuild / organization := "com.messenger"
ThisBuild / version      := "0.0.1"

val PekkoVersion           = "1.5.0"
val PekkoHttpVersion       = "1.3.0"
val PekkoManagementVersion = "1.2.1"
val LogbackVersion         = "1.5.32"

val pekkoActorTyped      = "org.apache.pekko" %% "pekko-actor-typed"            % PekkoVersion
val pekkoStream          = "org.apache.pekko" %% "pekko-stream"                 % PekkoVersion
val pekkoStreamTyped     = "org.apache.pekko" %% "pekko-stream-typed"           % PekkoVersion
val pekkoClusterTyped    = "org.apache.pekko" %% "pekko-cluster-typed"          % PekkoVersion
val pekkoClusterSharding = "org.apache.pekko" %% "pekko-cluster-sharding-typed" % PekkoVersion
val pekkoJackson         = "org.apache.pekko" %% "pekko-serialization-jackson"  % PekkoVersion
val pekkoSlf4j           = "org.apache.pekko" %% "pekko-slf4j"                  % PekkoVersion

val pekkoHttp          = "org.apache.pekko" %% "pekko-http"            % PekkoHttpVersion
val pekkoHttpSprayJson = "org.apache.pekko" %% "pekko-http-spray-json" % PekkoHttpVersion
val pekkoHttpCaching   = "org.apache.pekko" %% "pekko-http-caching"    % PekkoHttpVersion
val pekkoHttpJackson   = "org.apache.pekko" %% "pekko-http-jackson"    % PekkoHttpVersion
val pekkoHttpXml       = "org.apache.pekko" %% "pekko-http-xml"        % PekkoHttpVersion

val pekkoDiscovery       = "org.apache.pekko" %% "pekko-discovery"                    % PekkoVersion
val pekkoManagement      = "org.apache.pekko" %% "pekko-management"                   % PekkoManagementVersion
val pekkoMgmtClusterHttp = "org.apache.pekko" %% "pekko-management-cluster-http"      % PekkoManagementVersion
val pekkoMgmtBootstrap   = "org.apache.pekko" %% "pekko-management-cluster-bootstrap" % PekkoManagementVersion
val pekkoK8sApi          = "org.apache.pekko" %% "pekko-discovery-kubernetes-api"     % PekkoManagementVersion

val logback = "ch.qos.logback" % "logback-classic" % LogbackVersion

val jbcrypt = "org.mindrot" % "jbcrypt" % "0.4"
val jwt = "com.auth0" % "java-jwt" % "4.5.0"

val hibernate = "org.hibernate" % "hibernate-core" % "7.2.0.Final"
val postgresql = "org.postgresql" % "postgresql" % "42.7.8"
val hibernate_hikari = "org.hibernate" % "hibernate-hikaricp" % "7.2.0.Final" pomOnly()
val flywaydb_core =  "org.flywaydb" % "flyway-core" % "10.13.0"
val flywaydb_postgresql =   "org.flywaydb" % "flyway-database-postgresql" % "10.13.0"

val pekkoPersistenceCassandra = "org.apache.pekko" %% "pekko-persistence-cassandra" % "1.1.0"
val pekkoTestKit          = "org.apache.pekko" %% "pekko-actor-testkit-typed"  % PekkoVersion % Test
val pekkoPersistenceTest  = "org.apache.pekko" %% "pekko-persistence-testkit"  % PekkoVersion % Test
val junitJupiter           = "org.junit.jupiter"       % "junit-jupiter"         % "5.10.0"  % Test
val jupiterInterface       = "com.github.sbt.junit"    % "jupiter-interface"     % "0.15.1"  % Test
val mockitoCore            = "org.mockito"             % "mockito-core"          % "5.10.0"  % Test
val pekkoPersistenceQuery    = "org.apache.pekko" %% "pekko-persistence-query"      % PekkoVersion
val pekkoPersistenceTyped    = "org.apache.pekko" %% "pekko-persistence-typed"      % PekkoVersion
val cassandraDriver          = "com.datastax.oss" % "java-driver-core"              % "4.17.0"

val sharedLibs = Seq(
  pekkoActorTyped,
  pekkoJackson,
  pekkoClusterSharding,
  pekkoHttpSprayJson,
  pekkoDiscovery,
  pekkoManagement,
  pekkoMgmtClusterHttp,
  pekkoMgmtBootstrap,
  pekkoK8sApi
)

val gatewayLibs = Seq(
  pekkoClusterTyped,
  pekkoStream,
  pekkoStreamTyped,
  pekkoSlf4j,
  pekkoHttp,
  pekkoHttpCaching,
  pekkoHttpJackson,
  pekkoHttpXml,
  logback
)

val coreLibs = Seq(
  pekkoClusterTyped,
  pekkoStream,
  pekkoSlf4j,
  pekkoHttp,
  pekkoHttpCaching,
  pekkoHttpJackson,
  pekkoHttpXml,
  logback,
  pekkoPersistenceCassandra,
  pekkoPersistenceQuery,
  pekkoPersistenceTyped,
  pekkoPersistenceQuery,
  pekkoPersistenceTyped,
  cassandraDriver
)

val authLibs = Seq(
  pekkoActorTyped,
  pekkoClusterTyped,
  pekkoClusterSharding,
  pekkoStream,
  logback,
  pekkoSlf4j,
  jbcrypt,
  jwt,
  hibernate,
  hibernate_hikari,
  postgresql,
  flywaydb_core,
  flywaydb_postgresql
)


lazy val commonPackagingSettings = Seq(
  daemonUser := "george",
  maintainer := "George Filatov <liggidarck@gmail.com>",
  dockerBaseImage := "eclipse-temurin:21-jre",
  // 8080 - HTTP (gateway/api)
  // 8558 - Pekko Management (Health checks & Discovery)
  // 25520 - Pekko Remoting
  // 9095 - Prometheus
  dockerExposedPorts := Seq(8080, 8558, 25520, 9095),
  Docker / packageName := s"messenger/${name.value}",
  dockerUpdateLatest := true,

  Docker / dockerBuildCommand := {
    dockerExecCommand.value ++ Seq("buildx", "build", "--load") ++ dockerBuildOptions.value :+ "."
  }
)

lazy val root = (project in file("."))
  .aggregate(shared, gateway, chat_core, auth)
  .settings(
    name := "messenger-cluster-system",
    publish / skip := true
  )

lazy val shared = (project in file("messenger-shared"))
  .settings(
    name := "messenger-shared-messages",
    libraryDependencies ++= sharedLibs
  )

lazy val gateway = (project in file("messenger-api-gateway"))
  .dependsOn(shared)
  .enablePlugins(JavaServerAppPackaging, SystemVPlugin)
  .settings(
    commonPackagingSettings,
    name := "messenger-gateway-node",
    packageName := name.value,
    packageSummary := "messenger's gateway cluster node",
    packageDescription := "The http entrypoint to cluster",
    Compile / mainClass := Some("com.messenger.main.Main"),
    Compile / doc / sources := Seq.empty,
    libraryDependencies ++= gatewayLibs
  )

lazy val chat_core = (project in file ("messenger-chat-core"))
  .dependsOn(shared)
  .enablePlugins(JavaServerAppPackaging, SystemVPlugin)
  .settings(
    commonPackagingSettings,
    name := "messenger-chat-core",
    packageName := name.value,
    packageDescription := "core messenger",
    Compile / mainClass := Some("com.messenger.main.Main"),
    Compile / doc / sources := Seq.empty,
    libraryDependencies ++= coreLibs ++ Seq(pekkoTestKit, pekkoPersistenceTest, junitJupiter, mockitoCore, jupiterInterface),
    Test / fork := false,
    Test / testOptions ++= Seq(
      Tests.Argument(jupiterTestFramework, "-v")
    )
  )

lazy val auth = (project in file("messenger-auth"))
  .dependsOn(shared)
  .enablePlugins(JavaServerAppPackaging, SystemVPlugin, FlywayPlugin)
  .settings(
    commonPackagingSettings,
    name := "messenger-auth",
    packageName := name.value,
    packageDescription := "auth messenger",
    Compile / mainClass := Some("com.messenger.main.Main"),
    Compile / doc / sources := Seq.empty,

    flywayUrl := sys.env.getOrElse("DB_URL", s"jdbc:postgresql://${sys.env.getOrElse("DB_HOST", "127.0.0.1")}:5432/${sys.env.getOrElse("DB_NAME", "messenger_auth")}"),
    flywayUser := sys.env.getOrElse("DB_USER", "messenger_user"),
    flywayPassword := sys.env.getOrElse("DB_PASSWORD", "super_secure_pass"),
    flywaySchemas := Seq("common"),

    libraryDependencies ++= authLibs
  )

addCommandAlias("pubAll", ";gateway/docker:publishLocal;auth/docker:publishLocal;chat_core/Docker/publishLocal")