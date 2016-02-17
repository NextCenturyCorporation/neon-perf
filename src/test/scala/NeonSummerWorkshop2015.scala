
import scala.concurrent.duration._

import io.gatling.core.Predef._
import io.gatling.http.Predef._
import io.gatling.jdbc.Predef._
import scala.util.Random
import org.joda.time.{Period, DateTime}

import Joda._


class NeonSummerWorkshop2015 extends Simulation {

    val serverUri = System.getProperty("neon.server")
    if (serverUri == null) {
      throw new IllegalArgumentException("The system property 'neon.server' must be specified (e.g., by -Dneon.server=http://example.com:8080)")
    }
	val httpProtocol = http
		.baseURL(serverUri)
		.inferHtmlResources(BlackList(""".*\.js""", """.*\.css""", """.*\.png""", """.*\.html""", """.*\.gif""", """.*\.jpeg""", """.*\.jpg""", """.*\.ico""", """.*\.woff""", """.*\.(t|o)tf"""), WhiteList())

	val headers_0 = Map(
		"Accept" -> "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
		"Pragma" -> "no-cache")

	val headers_1 = Map("X-Requested-With" -> "XMLHttpRequest")

	val headers_2 = Map(
		"Pragma" -> "no-cache",
        "Content-Type" -> "application/json",
		"X-Requested-With" -> "XMLHttpRequest")

	val headers_3 = Map(
		"Accept" -> "*/*",
		"X-Requested-With" -> "XMLHttpRequest")

	val headers_22 = Map(
		"Content-Type" -> "text/plain; charset=UTF-8",
		"Pragma" -> "no-cache",
		"X-Requested-With" -> "XMLHttpRequest")

    val uri1 = serverUri + "/neon"

    val databaseType = "elasticsearch"

    val taxiTimeSeries = StringBody("""
      {
        "sortClauses": [ { "sortOrder": 1, "fieldName": "date" } ],
        "filter": {
          "whereClause": {
            "whereClauses": [
              { "rhs": "1970-01-01T00:00:00.000Z", "operator": ">=", "lhs": "pickup_datetime", "type": "where" },
              { "rhs": "2025-01-01T00:00:00.000Z", "operator": "<=", "lhs": "pickup_datetime", "type": "where" }
            ],
            "type": "and"
          },
          "tableName": "trip",
          "databaseName": "nyouterboro"
        },
        "fields": [ "*" ],
        "ignoreFilters_": false,
        "selectionOnly_": false,
        "ignoredFilterIds_": [],
        "groupByClauses": [
          { "name": "year", "field": "pickup_datetime", "operation": "year", "type": "function" },
          { "name": "month", "field": "pickup_datetime", "operation": "month", "type": "function" },
          { "name": "day", "field": "pickup_datetime", "operation": "dayOfMonth", "type": "function" }
        ],
        "isDistinct": false,
        "aggregates": [
          { "name": "value", "field": "*", "operation": "count" },
          { "name": "date", "field": "pickup_datetime", "operation": "min" }
        ]
      }
    """)
    val taxiTimeSeriesRequest = http("taxiTimeSeries")
			.post(uri1 + "/services/queryservice/query/localhost/" + databaseType)
			.headers(headers_2)
			.body(taxiTimeSeries)

    val taxiTripLengthTimeSeries = StringBody("""
      {
        "sortClauses": [ { "sortOrder": 1, "fieldName": "date" } ],
        "filter": {
          "whereClause": {
            "whereClauses": [
              { "rhs": "1970-01-01T00:00:00.000Z", "operator": ">=", "lhs": "pickup_datetime", "type": "where" },
              { "rhs": "2025-01-01T00:00:00.000Z", "operator": "<=", "lhs": "pickup_datetime", "type": "where" }
            ],
            "type": "and"
          },
          "tableName": "trip",
          "databaseName": "nyouterboro"
        },
        "fields": [ "*" ],
        "ignoreFilters_": false,
        "selectionOnly_": false,
        "ignoredFilterIds_": [],
        "groupByClauses": [
          { "name": "year", "field": "pickup_datetime", "operation": "year", "type": "function" },
          { "name": "month", "field": "pickup_datetime", "operation": "month", "type": "function" },
          { "name": "day", "field": "pickup_datetime", "operation": "dayOfMonth", "type": "function" }
        ],
        "isDistinct": false,
        "aggregates": [
          {"operation": "avg", "field": "trip_time_in_seconds", "name": "value"},
          { "name": "date", "field": "pickup_datetime", "operation": "min" }
        ]
      }
    """)
    val taxiTripLengthTimeSeriesRequest = http("taxiTripLengthTimeSeries")
			.post(uri1 + "/services/queryservice/query/localhost/" + databaseType)
			.headers(headers_2)
			.body(taxiTripLengthTimeSeries)

    val taxiSpeedTimeSeries = StringBody("""
      {
        "sortClauses": [ { "sortOrder": 1, "fieldName": "date" } ],
        "filter": {
          "whereClause": {
            "whereClauses": [
              { "rhs": "1970-01-01T00:00:00.000Z", "operator": ">=", "lhs": "pickup_datetime", "type": "where" },
              { "rhs": "2025-01-01T00:00:00.000Z", "operator": "<=", "lhs": "pickup_datetime", "type": "where" }
            ],
            "type": "and"
          },
          "tableName": "trip",
          "databaseName": "nyouterboro"
        },
        "fields": [ "*" ],
        "ignoreFilters_": false,
        "selectionOnly_": false,
        "ignoredFilterIds_": [],
        "groupByClauses": [
          { "name": "year", "field": "pickup_datetime", "operation": "year", "type": "function" },
          { "name": "month", "field": "pickup_datetime", "operation": "month", "type": "function" },
          { "name": "day", "field": "pickup_datetime", "operation": "dayOfMonth", "type": "function" }
        ],
        "isDistinct": false,
        "aggregates": [
          {"operation": "avg", "field": "calculated_speed_mph", "name": "value"},
          { "name": "date", "field": "pickup_datetime", "operation": "min" }
        ]
      }
    """)
    val taxiSpeedTimeSeriesRequest = http("taxiSpeedTimeSeries")
			.post(uri1 + "/services/queryservice/query/localhost/" + databaseType)
			.headers(headers_2)
			.body(taxiSpeedTimeSeries)

    val tweetTimeSeriesExtended = StringBody("""
      {
        "sortClauses": [ { "sortOrder": 1, "fieldName": "date" } ],
        "filter": {
          "whereClause": {
            "whereClauses": [
              { "rhs": "1970-01-01T00:00:00.000Z", "operator": ">=", "lhs": "created_at", "type": "where" },
              { "rhs": "2025-01-01T00:00:00.000Z", "operator": "<=", "lhs": "created_at", "type": "where" }
            ],
            "type": "and"
          },
          "tableName": "tweet",
          "databaseName": "nyctwitter"
        },
        "fields": [ "*" ],
        "ignoreFilters_": false,
        "selectionOnly_": false,
        "ignoredFilterIds_": [ "date-nyctwitter-tweet-0d901d1e-f620-416e-9bb3-4ea275d1329b" ],
        "groupByClauses": [
          { "name": "year", "field": "created_at", "operation": "year", "type": "function" },
          { "name": "month", "field": "created_at", "operation": "month", "type": "function" },
          { "name": "day", "field": "created_at", "operation": "dayOfMonth", "type": "function" }
        ],
        "isDistinct": false,
        "aggregates": [
          { "name": "count", "field": "*", "operation": "count" },
          { "name": "date", "field": "created_at", "operation": "min" }
        ]
      }
    """)
    val tweetTimeSeriesExtendedRequest = http("tweetTimeSeriesExtended")
			.post(uri1 + "/services/queryservice/query/localhost/" + databaseType + "/?ignoredFilterIds=date-nyctwitter-tweet-772326c4-995b-44f2-aa08-6e9801288b36")
			.headers(headers_2)
			.body(tweetTimeSeriesExtended)

    val get500 = StringBody("""
      {
        "limitClause": {
          "limit": 500
        },
        "sortClauses": [ ],
        "filter": {
          "tableName": "tweet",
          "databaseName": "nyctwitter"
        },
        "fields": [
          "*"
        ],
        "ignoreFilters_": false,
        "selectionOnly_": false,
        "ignoredFilterIds_": [],
        "groupByClauses": [],
        "isDistinct": false,
        "aggregates": []
      }
    """)
    val get500Request = http("get500")
			.post(uri1 + "/services/queryservice/query/localhost/" + databaseType)
			.headers(headers_2)
			.body(get500)

    val temperature = StringBody("""
      {
        "aggregates": [
          {"operation": "avg", "field": "Mean TemperatureF", "name": "value"},
          {"operation": "min", "field": "Date", "name": "date"}
        ],
        "fields": ["*"],
        "filter": {
          "databaseName": "nycweather",
          "tableName": "day",
          "whereClause": {
            "type": "and",
            "whereClauses": [
              {"type": "where", "lhs": "Date", "operator": ">=", "rhs": "1970-01-01T00:00:00.000Z"},
              {"type": "where", "lhs": "Date", "operator": "<=", "rhs": "2025-01-01T00:00:00.000Z"}
            ]
          }
        },
        "groupByClauses": [
          {"type": "function", "operation": "year", "field": "Date", "name": "year"},
          {"type": "function", "operation": "month", "field": "Date", "name": "month"},
          {"type": "function", "operation": "dayOfMonth", "field": "Date", "name": "day"}
        ],
        "ignoreFilters_": false,
        "ignoredFilterIds_": [],
        "isDistinct": false,
        "selectionOnly_": false,
        "sortClauses": [{"fieldName": "date", "sortOrder": 1}]
      }
    """)
    val temperatureRequest = http("temperature")
            .post(uri1 + "/services/queryservice/query/localhost/" + databaseType)
            .headers(headers_2)
            .body(temperature)

    val windSpeed = StringBody("""
      {
        "sortClauses": [ { "sortOrder": 1, "fieldName": "date" } ],
        "filter": {
          "whereClause": {
            "whereClauses": [
              { "rhs": "1970-01-01T00:00:00.000Z", "operator": ">=", "lhs": "Date", "type": "where" },
              { "rhs": "2025-01-01T00:00:00.000Z", "operator": "<=", "lhs": "Date", "type": "where" }
            ],
            "type": "and"
          },
          "tableName": "day",
          "databaseName": "nycweather"
        },
        "fields": [ "*" ],
        "ignoreFilters_": false,
        "selectionOnly_": false,
        "ignoredFilterIds_": [],
        "groupByClauses": [
          { "name": "year", "field": "Date", "operation": "year", "type": "function" },
          { "name": "month", "field": "Date", "operation": "month", "type": "function" },
          { "name": "day", "field": "Date", "operation": "dayOfMonth", "type": "function" }
        ],
        "isDistinct": false,
        "aggregates": [
          { "name": "value", "field": "Mean Wind SpeedMPH", "operation": "avg" },
          { "name": "date", "field": "Date", "operation": "min" }
        ]
      }
    """)
    val windSpeedRequest = http("windSpeed")
			.post(uri1 + "/services/queryservice/query/localhost/" + databaseType)
			.headers(headers_2)
			.body(windSpeed)

    val cloudCover = StringBody("""
      {
        "sortClauses": [ { "sortOrder": 1, "fieldName": "date" } ],
        "filter": {
          "whereClause": {
            "whereClauses": [
              { "rhs": "1970-01-01T00:00:00.000Z", "operator": ">=", "lhs": "Date", "type": "where" },
              { "rhs": "2025-01-01T00:00:00.000Z", "operator": "<=", "lhs": "Date", "type": "where" }
            ],
            "type": "and"
          },
          "tableName": "day",
          "databaseName": "nycweather"
        },
        "fields": [ "*" ],
        "ignoreFilters_": false,
        "selectionOnly_": false,
        "ignoredFilterIds_": [],
        "groupByClauses": [
          { "name": "year", "field": "Date", "operation": "year", "type": "function" },
          { "name": "month", "field": "Date", "operation": "month", "type": "function" },
          { "name": "day", "field": "Date", "operation": "dayOfMonth", "type": "function" }
        ],
        "isDistinct": false,
        "aggregates": [
          { "name": "value", "field": "CloudCover", "operation": "avg" },
          { "name": "date", "field": "Date", "operation": "min" }
        ]
      }
    """)
    val cloudCoverRequest = http("cloudCover")
			.post(uri1 + "/services/queryservice/query/localhost/" + databaseType)
			.headers(headers_2)
			.body(cloudCover)

    val precipitation = StringBody("""
      {
        "sortClauses": [ { "sortOrder": 1, "fieldName": "date" } ],
        "filter": {
          "whereClause": {
            "whereClauses": [
              { "rhs": "1970-01-01T00:00:00.000Z", "operator": ">=", "lhs": "Date", "type": "where" },
              { "rhs": "2025-01-01T00:00:00.000Z", "operator": "<=", "lhs": "Date", "type": "where" }
            ],
            "type": "and"
          },
          "tableName": "day",
          "databaseName": "nycweather"
        },
        "fields": [ "*" ],
        "ignoreFilters_": false,
        "selectionOnly_": false,
        "ignoredFilterIds_": [],
        "groupByClauses": [
          { "name": "year", "field": "Date", "operation": "year", "type": "function" },
          { "name": "month", "field": "Date", "operation": "month", "type": "function" },
          { "name": "day", "field": "Date", "operation": "dayOfMonth", "type": "function" }
        ],
        "isDistinct": false,
        "aggregates": [
          { "name": "value", "field": "PrecipitationIn", "operation": "avg" },
          { "name": "date", "field": "Date", "operation": "min" }
        ]
      }
    """)
    val precipitationRequest = http("precipitation")
			.post(uri1 + "/services/queryservice/query/localhost/" + databaseType)
			.headers(headers_2)
			.body(precipitation)

    val getTwitter5000 = StringBody("""
      {
        "limitClause": { "limit": 5000 },
        "sortClauses": [],
        "filter": { "tableName": "tweet", "databaseName": "nyctwitter" },
        "fields": [ "*" ],
        "ignoreFilters_": false,
        "selectionOnly_": false,
        "ignoredFilterIds_": [],
        "groupByClauses": [],
        "isDistinct": false,
        "aggregates": []
      }
    """)
    val getTwitter5000Request = http("getTwitter5000")
			.post(uri1 + "/services/queryservice/query/localhost/" + databaseType)
			.headers(headers_2)
			.body(getTwitter5000)

    val getTaxi10000 = StringBody("""
      {
        "limitClause": { "limit": 5000 },
        "sortClauses": [],
        "filter": { "tableName": "trip", "databaseName": "nyouterboro" },
        "fields": [ "*" ],
        "ignoreFilters_": false,
        "selectionOnly_": false,
        "ignoredFilterIds_": [],
        "groupByClauses": [],
        "isDistinct": false,
        "aggregates": []
      }
    """)
    val getTaxi10000Request = http("getTaxi10000")
			.post(uri1 + "/services/queryservice/query/localhost/" + databaseType)
			.headers(headers_2)
			.body(getTaxi10000)

    val tagsRequest = http("tags")
            .get(uri1 + "/services/queryservice/arraycounts/localhost/" + databaseType + "/nyctwitter/tweet?field=lowertags&limit=40")
    val getFiltersRequest = http("getFilters")
			.get(uri1 + "/services/filterservice/filters/*/*")
			.headers(headers_1)

    val timeFilter = StringBody("""
      {
        "filter": {
          "whereClause": {
            "whereClauses": [
              {
                "rhs": "${startDate}",
                "operator": ">=",
                "lhs": "time",
                "type": "where"
              },
              {
                "rhs": "${endDate}",
                "operator": "<",
                "lhs": "time",
                "type": "where"
              }
            ],
            "type": "and"
          },
          "tableName": "earthquakes",
          "databaseName": "test",
          "filterName": "Timeline - Earthquakes: 5/24/2014 to 6/6/2014"
        },
        "id": "date-test-earthquakes-46d60c31-3f27-40c7-80cf-61a581af5545"
      }
    """)
    val startDate = DateTime.parse("2014-01-01T00:00:00.000Z")
    val endDate = DateTime.parse("2014-12-31T00:00:00.000Z")
    val daily = new Period(0,0,0,1,0,0,0,0)
    def dateRange(from: DateTime, to: DateTime, step: Period): Iterator[DateTime]=Iterator.iterate(from)(_.plus(step)).takeWhile(!_.isAfter(to))

    def reservoirSample[A:Manifest](iterator:Iterator[A], k:Int ): List[A] = {
      val result = new Array[A](k)

      for ((item, index) <- iterator.zipWithIndex) {
        if (index < k) {
          result(index) = item
        } else {
          val s = Random.nextInt(index)
          if (s < k) {
            result(s) = item
          }
        }
      }
      return result.toList
    }

    val timeFeeder = Iterator.continually({
      val validDates = dateRange(startDate, endDate, daily)
      val range = reservoirSample(validDates, 2).sorted
      Map(
        "startDate" -> range.head.toString(),
        "endDate" -> range.tail.head.toString()
      )
    })

    val mapFilter = StringBody("""
      {
        "filter": {
          "whereClause": {
            "whereClauses": [
              {
                "rhs": ${westLongitude},
                "operator": ">=",
                "lhs": "longitude",
                "type": "where"
              },
              {
                "rhs": ${eastLongitude},
                "operator": "<=",
                "lhs": "longitude",
                "type": "where"
              },
              {
                "rhs": ${southLatitude},
                "operator": ">=",
                "lhs": "latitude",
                "type": "where"
              },
              {
                "rhs": ${northLatitude},
                "operator": "<=",
                "lhs": "latitude",
                "type": "where"
              }
            ],
            "type": "and"
          },
          "tableName": "earthquakes",
          "databaseName": "test",
          "filterName": "Map - Earthquakes"
        },
        "id": "map-test-earthquakes-945f3210-882a-4014-83e9-08b5e34b3fe9"
      }
    """)
    val latitudeMin = -74.0370788
    val latitudeMax = -73.7033691
    val longitudeMin = 40.5611638
    val longitudeMax = 40.9139096

    def twoDoublesInRange(from: Double, to: Double): Seq[Double] = {
      var doubles = for (i <- 0 to 1) yield from + (to - from) * Random.nextDouble
      return doubles.sorted
    }

    val mapFeeder = Iterator.continually({
      val latitudes = twoDoublesInRange(latitudeMin, latitudeMax)
      var longitudes = twoDoublesInRange(longitudeMin, longitudeMax)

      Map(
        "northLatitude" -> longitudes.tail.head,
        "southLatitude" -> longitudes.head,
        "westLongitude" -> longitudes.head,
        "eastLongitude" -> longitudes.tail.head
      )
    })

    val barChartFilter = StringBody("""
      {
        "filter": {
          "whereClause": {
            "rhs": "${netType}",
            "operator": "=",
            "lhs": "net",
            "type": "where"
          },
          "tableName": "earthquakes",
          "databaseName": "test",
          "filterName": "BarChart - Earthquakes: Net = ak"
        },
        "id": "barchart-test-earthquakes-2b74b1f7-15a1-4124-80bd-46f99eb484c6"
      }
    """)

    val barChartFeeder = Array(
      Map("netType" -> "ak"),
      Map("netType" -> "ci"),
      Map("netType" -> "hv"),
      Map("netType" -> "ld"),
      Map("netType" -> "mb"),
      Map("netType" -> "nc"),
      Map("netType" -> "nm"),
      Map("netType" -> "nn"),
      Map("netType" -> "pr"),
      Map("netType" -> "se"),
      Map("netType" -> "us"),
      Map("netType" -> "uu"),
      Map("netType" -> "uw")
    ).random

    val updateResources = Seq(
      getFiltersRequest,
      taxiTimeSeriesRequest,
      taxiTripLengthTimeSeriesRequest,
      taxiSpeedTimeSeriesRequest,
      tweetTimeSeriesExtendedRequest,
      temperatureRequest,
      windSpeedRequest,
      cloudCoverRequest,
      precipitationRequest,
      getTwitter5000Request,
      getTaxi10000Request,
      get500Request,
      tagsRequest
    )

    val updateResourcesWithoutTime = updateResources.filterNot(e => (e eq tweetTimeSeriesExtendedRequest))
    //val updateResourcesWithoutBarChart = updateResources.filterNot(e => e eq groupByNetBarRequest)

	val scn = scenario("NeonSummerWorkshop2015")
		// Initial load
		.exec(http("Load page")
			.get("/neon-gtd/app/")
			.headers(headers_0))
		.pause(3, 5)
		.exec(http("Get table and fields")
			.get("/neon/services/queryservice/tablesandfields/localhost/" + databaseType + "/test")
			.headers(headers_1)
			.resources(http("Clear filters")
			.post(uri1 + "/services/filterservice/clearfilters")
			.headers(headers_2)))
		.pause(1)
		.exec(http("Get filter builder id")
			.get("/neon/services/widgetservice/instanceid?qualifier=filterBuilder")
			.headers(headers_3)
			.resources(updateResources:_*))
        .repeat(10) {
          pause(9, 15)
          // Time Filter
          .feed(timeFeeder)
          .exec(http("timeFilter")
              .post("/neon/services/filterservice/replacefilter")
              .headers(headers_2)
              .body(timeFilter)
              .resources(updateResourcesWithoutTime:_*))
          .pause(8, 12)
          // Remove Time Filter
          .exec(http("request_22")
              .post("/neon/services/filterservice/removefilter")
              .headers(headers_22)
              .body(StringBody("date-test-earthquakes-46d60c31-3f27-40c7-80cf-61a581af5545"))
              .resources(updateResourcesWithoutTime:_*))
          .pause(6, 8)
          // Map Filter
          .feed(mapFeeder)
          .exec(http("request_30")
              .post("/neon/services/filterservice/replacefilter")
              .headers(headers_2)
              .body(mapFilter)
              .resources(updateResources:_*))
          .pause(7, 9)
          // Remove Map FIlter
          .exec(http("request_48")
              .post("/neon/services/filterservice/removefilter")
              .headers(headers_22)
              .body(StringBody("map-test-earthquakes-945f3210-882a-4014-83e9-08b5e34b3fe9"))
              .resources(updateResources:_*))
          /*
          .pause(12, 18)
          // Network Filter
          .feed(barChartFeeder)
          .exec(http("request_62")
              .post("/neon/services/filterservice/addfilter")
              .headers(headers_2)
              .body(barChartFilter)
              .resources(updateResourcesWithoutBarChart:_*))
          .pause(6, 8)
          // Remove Network Filter
          .exec(http("request_71")
              .post("/neon/services/filterservice/removefilter")
              .headers(headers_22)
              .body(StringBody("barchart-test-earthquakes-2b74b1f7-15a1-4124-80bd-46f99eb484c6"))
              .resources(updateResourcesWithoutBarChart:_*))
          */
          .pause(15, 23)
          // Second Map Filter
          .feed(mapFeeder)
          .exec(http("request_80")
              .post("/neon/services/filterservice/replacefilter")
              .headers(headers_2)
              .body(mapFilter)
              .resources(updateResources:_*))
          .pause(6, 8)
          // Map and Time Filter
          .feed(timeFeeder)
          .exec(http("request_98")
              .post("/neon/services/filterservice/replacefilter")
              .headers(headers_2)
              .body(timeFilter)
              .resources(updateResourcesWithoutTime:_*))
          .pause(8, 12)
          .feed(barChartFeeder)
          /*
          // Map Time and Network Filter
          .exec(http("request_106")
              .post("/neon/services/filterservice/addfilter")
              .headers(headers_2)
              .body(barChartFilter)
              .resources(updateResourcesWithoutBarChart:_*))
          // Remove Filters
          .exec(http("request_71")
              .post("/neon/services/filterservice/removefilter")
              .headers(headers_22)
              .body(StringBody("barchart-test-earthquakes-2b74b1f7-15a1-4124-80bd-46f99eb484c6"))
              .resources(updateResourcesWithoutBarChart:_*))
          */
          .exec(http("request_22")
              .post("/neon/services/filterservice/removefilter")
              .headers(headers_22)
              .body(StringBody("date-test-earthquakes-46d60c31-3f27-40c7-80cf-61a581af5545"))
              .resources(updateResourcesWithoutTime:_*))
          .exec(http("request_48")
              .post("/neon/services/filterservice/removefilter")
              .headers(headers_22)
              .body(StringBody("map-test-earthquakes-945f3210-882a-4014-83e9-08b5e34b3fe9"))
              .resources(updateResources:_*))
        }

    def getNumUsers(): Int = {
      try {
        return System.getProperty("neon.users").toInt
      } catch {
        case e: Exception => return 1
      }
    }
    setUp(scn.inject(rampUsers(getNumUsers()) over (1 minutes))).protocols(httpProtocol)
}
