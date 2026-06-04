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

final long WINDOW_MS = 30_000L   // 30-second duplicate window

// Read FlowFile content
def content = ""
session.read(flowFile, { inputStream ->
    content = IOUtils.toString(inputStream, StandardCharsets.UTF_8)
} as InputStreamCallback)

def parsed = new JsonSlurper().parseText(content)
def record = (parsed instanceof List) ? parsed[0] : parsed

def accountMasked = record?.account_masked?.toString() ?: ""
def amount        = record?.amount?.toString()         ?: ""
def merchantId    = record?.merchant_id?.toString()    ?: ""

if (!accountMasked || !amount || !merchantId) {
    session.transfer(flowFile, REL_SUCCESS)
    return
}

// NiFi 1.23 Netty-based DMC: Deserializer takes byte[], not InputStream
def strSer = [serialize: { val, os ->
    os.write(val.toString().getBytes(StandardCharsets.UTF_8))
}] as Serializer

def strDe = [deserialize: { byte[] value ->
    new String(value, StandardCharsets.UTF_8)
}] as Deserializer

def serviceId = context.getProperty("DMCServiceId").getValue()
def dmc = context.controllerServiceLookup.getControllerService(serviceId) as DistributedMapCacheClient

// "dup:" prefix avoids collision with Rule 1's "vel:" keys
def cacheKey = "dup:${accountMasked}:${amount}:${merchantId}"
def now      = System.currentTimeMillis()
boolean isFraud = false

try {
    def cached = dmc.get(cacheKey, strSer, strDe)
    if (cached) {
        def e  = new JsonSlurper().parseText(cached)
        long firstSeen = (e.ts as long) ?: 0L
        if ((now - firstSeen) <= WINDOW_MS) {
            // Same combo within 30s — duplicate
            isFraud = true
        }
        // Window expired: treat as first occurrence, reset below
    }

    if (!isFraud) {
        // Store/refresh timestamp for this combo
        dmc.put(cacheKey, JsonOutput.toJson([ts: now]), strSer, strSer)
    }
} catch (Exception ex) {
    def sw = new java.io.StringWriter()
    ex.printStackTrace(new java.io.PrintWriter(sw))
    log.error("Duplicate DMC error: ${sw}")
    session.transfer(flowFile, REL_SUCCESS)
    return
}

if (isFraud) {
    def alertId = "ALT-D-" + UUID.randomUUID().toString().replace("-","").substring(0,8).toUpperCase()

    // Write fraud_flag=true + alert_id into JSON content for PutDatabaseRecord
    def updatedRecord = [:]
    record.each { k, v -> updatedRecord[k] = v }
    updatedRecord['fraud_flag'] = true
    updatedRecord['alert_id']   = alertId
    def newContent = JsonOutput.toJson([updatedRecord])

    flowFile = session.write(flowFile, { os ->
        os.write(newContent.getBytes(StandardCharsets.UTF_8))
    } as OutputStreamCallback)

    // Set attributes for PutSQL (fact_alert) downstream
    flowFile = session.putAttribute(flowFile, 'rule_triggered', 'duplicate')
    flowFile = session.putAttribute(flowFile, 'severity',       'LOW')
    flowFile = session.putAttribute(flowFile, 'alert_id',       alertId)
    flowFile = session.putAttribute(flowFile, 'fraud_flag',     'true')
    flowFile = session.putAttribute(flowFile, 'txn_id',         record.transaction_id?.toString() ?: '')
    session.transfer(flowFile, REL_FAILURE)
} else {
    session.transfer(flowFile, REL_SUCCESS)
}
