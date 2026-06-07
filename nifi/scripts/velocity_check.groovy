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

final long WINDOW_MS = 60_000L   // 60-second velocity window (sliding)
final int  THRESHOLD = 3          // >= 3 transactions within the window triggers alert

// Read FlowFile content (JSON array [{...}] from Jolt + LookupRecord)
def content = ""
session.read(flowFile, { inputStream ->
    content = IOUtils.toString(inputStream, StandardCharsets.UTF_8)
} as InputStreamCallback)

def parsed = new JsonSlurper().parseText(content)
def record = (parsed instanceof List) ? parsed[0] : parsed
def accountMasked = record?.account_masked?.toString() ?: ""

if (!accountMasked) {
    session.transfer(flowFile, REL_SUCCESS)
    return
}

// NiFi 1.23 uses Netty-based DMC:
//   Serializer.serialize(T value, OutputStream output)  — unchanged
//   Deserializer.deserialize(byte[] value)              — byte[], NOT InputStream
def strSer = [serialize: { val, os ->
    os.write(val.toString().getBytes(StandardCharsets.UTF_8))
}] as Serializer

def strDe = [deserialize: { byte[] value ->
    new String(value, StandardCharsets.UTF_8)
}] as Deserializer

def serviceId = context.getProperty("DMCServiceId").getValue()
def dmc = context.controllerServiceLookup.getControllerService(serviceId) as DistributedMapCacheClient

// Cache key prefixed with "vel:" to avoid collision with Rule 4 (duplicate)
def cacheKey = "vel:${accountMasked}"
def now      = System.currentTimeMillis()
boolean isFraud = false

// Sliding window: store list of timestamps, prune entries older than WINDOW_MS,
// then check if size >= THRESHOLD. Catches patterns that straddle window boundaries
// (e.g. t=0s, t=30s, t=90s where tumbling window would miss the [30s,90s] span).
try {
    def cached = dmc.get(cacheKey, strSer, strDe)
    List<Long> timestamps = []
    if (cached) {
        def e = new JsonSlurper().parseText(cached)
        timestamps = (e.ts as List<Long>) ?: []
    }
    // Prune timestamps outside the 60-second sliding window
    timestamps = timestamps.findAll { (now - (it as long)) <= WINDOW_MS }
    timestamps << now
    if (timestamps.size() >= THRESHOLD) isFraud = true
    dmc.put(cacheKey, JsonOutput.toJson([ts: timestamps]), strSer, strSer)
} catch (Exception ex) {
    // Fail-open: log error but don't block the pipeline
    def sw = new java.io.StringWriter()
    ex.printStackTrace(new java.io.PrintWriter(sw))
    log.error("Velocity DMC error: ${sw}")
    session.transfer(flowFile, REL_SUCCESS)
    return
}

if (isFraud) {
    def alertId = "ALT-V-" + UUID.randomUUID().toString().replace("-","").substring(0,8).toUpperCase()

    // Write fraud_flag=true and alert_id into JSON content so PutDatabaseRecord
    // picks them up correctly when inserting into fact_txn
    def updatedRecord = [:]
    record.each { k, v -> updatedRecord[k] = v }
    updatedRecord['fraud_flag'] = true
    updatedRecord['alert_id']   = alertId
    def newContent = JsonOutput.toJson([updatedRecord])

    flowFile = session.write(flowFile, { os ->
        os.write(newContent.getBytes(StandardCharsets.UTF_8))
    } as OutputStreamCallback)

    // Set FlowFile attributes for PutSQL (fact_alert) downstream
    flowFile = session.putAttribute(flowFile, 'rule_triggered', 'velocity')
    flowFile = session.putAttribute(flowFile, 'severity',       'MEDIUM')
    flowFile = session.putAttribute(flowFile, 'alert_id',       alertId)
    flowFile = session.putAttribute(flowFile, 'fraud_flag',     'true')
    flowFile = session.putAttribute(flowFile, 'txn_id',         record.transaction_id?.toString() ?: '')
    session.transfer(flowFile, REL_FAILURE)
} else {
    session.transfer(flowFile, REL_SUCCESS)
}
