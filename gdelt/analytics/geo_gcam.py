
# // be sure to include a dependency to the geohash library
# // here in the 1st para of zeppelin:
# // z.load("com.github.davidmoten:geo:0.7.1")
# // to use the geohash functionality in your code

# val GcamRaw = GkgFileRaw.select("GkgRecordID","V21Date","V15Tone","V2GCAM", "V1Locations")
#     GcamRaw.cache()
#     GcamRaw.registerTempTable("GcamRaw")
 
# def vgeoWrap (lat: Double, long: Double, len: Int): String = {
#     var ret = GeoHash.encodeHash(lat, long, len)
#     // select the length of the geohash, less than 12..
#     // it pulls in the library dependency from       
#     //   com.github.davidmoten:geo:0.7.1
#     return(ret)
# } // we wrap up the geohash function locally
 
# // we register the vGeoHash function for use in SQL 
# sqlContext.udf.register("vGeoHash", vgeoWrap(_:Double,_:Double,_:Int))
 
# val ExtractGcam = sqlContext.sql("""
#     select
#         GkgRecordID 
#     ,   V21Date
#     ,   split(V2GCAM, ",")                  as Array
#     ,   explode(split(V1Locations, ";"))    as LocArray
#     ,   regexp_replace(V15Tone, ",.*$", "") as V15Tone 
#        -- note we truncate off the other scores
#     from GcamRaw 
#     where length(V2GCAM) >1 and length(V1Locations) >1
# """)

# val explodeGcamDF = ExtractGcam.explode("Array", "GcamRow"){c: Seq[String] => c }
 
 
# val GcamRows = explodeGcamDF.select("GkgRecordID","V21Date","V15Tone","GcamRow", "LocArray")
# // note ALL the locations get repeated against
# // every GCAM sentiment row

#     GcamRows.registerTempTable("GcamRows")
 
# val TimeSeries = sqlContext.sql("""
# select   -- create geohash keys
#   d.V21Date
# , d.LocCountryCode
# , d.Lat
# , d.Long
#  , vGeoHash(d.Lat, d.Long, 12)        as GeoHash
# , 'E' as NewsLang
# , regexp_replace(Series, "\\.", "_") as Series
# , coalesce(sum(d.Value),0) as SumValue  
#            -- SQL’s "coalesce” means “replaces nulls with"
# , count(distinct  GkgRecordID )      as ArticleCount
# , Avg(V15Tone)                       as AvgTone
# from
# (   select  -- build Cartesian join of the series
#                 -- and granular locations
#       GkgRecordID
#     , V21Date
#     , ts_array[0]  as Series
#     , ts_array[1]  as Value
#     , loc_array[0] as LocType
#     , loc_array[2] as LocCountryCode
#     , loc_array[4] as Lat
#     , loc_array[5] as Long
#     , V15Tone
#     from
#        (select -- isolate the data to focus on
#          GkgRecordID
#        , V21Date
#        , split(GcamRow,   ":") as ts_array
#        , split(LocArray,  "#") as loc_array
#        , V15Tone
#        from GcamRows
#        where length(GcamRow)>1
#        ) x
#     where
#     (loc_array[0] = 3 or  loc_array[0] = 4) -- city level filter
# ) d
# group by 
#   d.V21Date
# , d.LocCountryCode
# , d.Lat
# , d.Long
# , vGeoHash(d.Lat, d.Long, 12)
# , d.Series
# order by 
#   d.V21Date
# , vGeoHash(d.Lat, d.Long, 12)
# , d.Series
# """)

