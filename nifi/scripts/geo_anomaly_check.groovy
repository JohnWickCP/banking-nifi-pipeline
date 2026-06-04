import org.apache.nifi.processor.io.InputStreamCallback
import org.apache.nifi.processor.io.OutputStreamCallback
import org.apache.commons.io.IOUtils
import org.apache.nifi.distributed.cache.client.DistributedMapCacheClient
import org.apache.nifi.distributed.cache.client.Serializer
import org.apache.nifi.distributed.cache.client.Deserializer
import groovy.json.JsonSlurper
import groovy.json.JsonOutput
import java.nio.charset.StandardCharsets

def flowFile = session.get()
if (!flowFile) return

final long WINDOW_MS   = 30 * 60 * 1000L   // 30-minute window
final double MAX_KM    = 300.0              // impossible distance threshold

// Haversine distance in km between two lat/lon points
static double haversine(double lat1, double lon1, double lat2, double lon2) {
    final double R = 6371.0
    double dLat = Math.toRadians(lat2 - lat1)
    double dLon = Math.toRadians(lon2 - lon1)
    double a = Math.sin(dLat / 2) * Math.sin(dLat / 2)
            + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2))
            * Math.sin(dLon / 2) * Math.sin(dLon / 2)
    return R * 2.0 * Math.atan2(Math.sqrt(a), Math.sqrt(1.0 - a))
}

// Read FlowFile content
def content = ""
session.read(flowFile, { inputStream ->
    content = IOUtils.toString(inputStream, StandardCharsets.UTF_8)
} as InputStreamCallback)

def parsed = new JsonSlurper().parseText(content)
def record = (parsed instanceof List) ? parsed[0] : parsed

def accountMasked = record?.account_masked?.toString() ?: ""
def latVal  = record?.merchant_lat
def lonVal  = record?.merchant_lon

if (!accountMasked || latVal == null || lonVal == null) {
    session.transfer(flowFile, REL_SUCCESS)
    return
}

double curLat = latVal as double
double curLon = lonVal as double

// NiFi 1.23 Netty-based DMC: Deserializer takes byte[], not InputStream
def strSer = [serialize: { val, os ->
    os.write(val.toString().getBytes(StandardCharsets.UTF_8))
}] as Serializer

def strDe = [deserialize: { byte[] value ->
    new String(value, StandardCharsets.UTF_8)
}] as Deserializer

def serviceId = context.getProperty("DMCServiceId").getValue()
def dmc = context.controllerServiceLookup.getControllerService(serviceId) as DistributedMapCacheClient

// "geo:" prefix avoids collision with Rule 1 "vel:" and Rule 4 "dup:" keys
def cacheKey = "geo:${accountMasked}"
def now      = System.currentTimeMillis()
boolean isFraud = false
double distKm   = 0.0

try {
    def cached = dmc.get(cacheKey, strSer, strDe)
    if (cached) {
        def e = new JsonSlurper().parseText(cached)
        long   prevTs  = (e.ts  as long)   ?: 0L
        double prevLat = (e.lat as double) ?: curLat
        double prevLon = (e.lon as double) ?: curLon

        long timeDiff = now - prevTs
        if (timeDiff > 0 && timeDiff <= WINDOW_MS) {
            distKm = haversine(prevLat, prevLon, curLat, curLon)
            if (distKm > MAX_KM) {
                isFraud = true
            }
        }
    }
    // Always update cache with current location
    dmc.put(cacheKey, JsonOutput.toJson([ts: now, lat: curLat, lon: curLon]), strSer, strSer)
} catch (Exception ex) {
    def sw = new java.io.StringWriter()
    ex.printStackTrace(new java.io.PrintWriter(sw))
    log.error("Geo-anomaly DMC error: ${sw}")
    session.transfer(flowFile, REL_SUCCESS)
    return
}

if (isFraud) {
    def alertId = "ALT-G-" + UUID.randomUUID().toString().replace("-","").substring(0,8).toUpperCase()

    def updatedRecord = [:]
    record.each { k, v -> updatedRecord[k] = v }
    updatedRecord['fraud_flag'] = true
    updatedRecord['alert_id']   = alertId
    def newContent = JsonOutput.toJson([updatedRecord])

    flowFile = session.write(flowFile, { os ->
        os.write(newContent.getBytes(StandardCharsets.UTF_8))
    } as OutputStreamCallback)

    flowFile = session.putAttribute(flowFile, 'rule_triggered', 'geo_anomaly')
    flowFile = session.putAttribute(flowFile, 'severity',       'HIGH')
    flowFile = session.putAttribute(flowFile, 'alert_id',       alertId)
    flowFile = session.putAttribute(flowFile, 'fraud_flag',     'true')
    flowFile = session.putAttribute(flowFile, 'txn_id',         record.transaction_id?.toString() ?: '')
    flowFile = session.putAttribute(flowFile, 'dist_km',        String.format("%.1f", distKm))
    session.transfer(flowFile, REL_FAILURE)
} else {
    session.transfer(flowFile, REL_SUCCESS)
}
