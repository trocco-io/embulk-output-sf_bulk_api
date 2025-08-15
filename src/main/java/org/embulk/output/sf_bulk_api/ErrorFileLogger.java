package org.embulk.output.sf_bulk_api;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.SerializedName;
import com.sforce.soap.partner.IError;
import com.sforce.soap.partner.sobject.SObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Salesforceエラーを収集し、構造化してファイルに出力するクラス
 */
public class ErrorFileLogger implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(ErrorFileLogger.class);
    private static final int FLUSH_INTERVAL = 100;
    
    private final String outputPath;
    private final int taskIndex;
    private final Gson gson;
    private boolean enabled;
    
    private BufferedWriter writer;
    private Path filePath;
    private int recordCount = 0;
    private int errorCount = 0;
    
    /**
     * エラーレコードの内部クラス
     */
    private static class ErrorRecord {
        @SerializedName("record_data")
        private final java.util.Map<String, Object> recordData;
        
        @SerializedName("error_code")
        private final String errorCode;
        
        @SerializedName("error_message")
        private final String errorMessage;
        
        @SerializedName("affected_fields")
        private final String[] affectedFields;
        
        @SerializedName("timestamp")
        private final String timestamp;
        
        @SerializedName("task_index")
        private final Integer taskIndex;
        
        @SerializedName("record_index")
        private final Integer recordIndex;
        
        ErrorRecord(java.util.Map<String, Object> recordData,
                   String errorCode, String errorMessage,
                   String[] affectedFields, Integer taskIndex, Integer recordIndex) {
            this.recordData = recordData;
            this.errorCode = errorCode != null ? errorCode : "";
            this.errorMessage = errorMessage != null ? errorMessage : "";
            this.affectedFields = affectedFields;
            this.timestamp = Instant.now().toString();
            this.taskIndex = taskIndex;
            this.recordIndex = recordIndex;
        }
    }
    
    public ErrorFileLogger(String outputPath, int taskIndex) {
        this.outputPath = outputPath;
        this.taskIndex = taskIndex;
        this.gson = new GsonBuilder().disableHtmlEscaping().create();
        this.enabled = outputPath != null && !outputPath.trim().isEmpty();
        
        
        if (enabled) {
            try {
                open();
            } catch (IOException e) {
                logger.error("Failed to open error file for writing", e);
                this.enabled = false;
            }
        }
    }
    
    private void open() throws IOException {
        if (!enabled || writer != null) {
            return;
        }
        
        this.filePath = Paths.get(generateFilePath());
        ensureDirectoryExists(filePath);
        
        this.writer = Files.newBufferedWriter(
            filePath,
            StandardCharsets.UTF_8,
            StandardOpenOption.CREATE,
            StandardOpenOption.APPEND
        );
        
    }
    
    /**
     * Salesforceエラーを記録
     */
    public void logError(SObject sObject, IError[] errors, int recordIndex) {
        if (!enabled || errors == null || errors.length == 0) {
            return;
        }
        java.util.Map<String, Object> recordData = extractAllFields(sObject);
        
        for (IError error : errors) {
            ErrorRecord record = new ErrorRecord(
                recordData,
                error.getStatusCode() != null ? error.getStatusCode().toString() : "",
                error.getMessage(),
                error.getFields(),
                taskIndex,
                recordIndex
            );
            
            writeRecord(record);
            errorCount++;
        }
    }
    
    
    private void writeRecord(ErrorRecord record) {
        if (writer == null) {
            return;
        }
        
        try {
            writer.write(gson.toJson(record));
            writer.newLine();
            recordCount++;
            
            if (recordCount % FLUSH_INTERVAL == 0) {
                writer.flush();
            }
        } catch (IOException e) {
            logger.error("Failed to write error record", e);
        }
    }
    
    /**
     * SObjectから全フィールドを抽出
     */
    private java.util.Map<String, Object> extractAllFields(SObject sObject) {
        java.util.Map<String, Object> fields = new java.util.HashMap<>();
        
        if (sObject == null) {
            return fields;
        }
        
        // SObjectの型を追加
        fields.put("_object_type", sObject.getType());
        
        // getChildren()で全フィールドを取得
        java.util.Iterator<com.sforce.ws.bind.XmlObject> children = sObject.getChildren();
        while (children != null && children.hasNext()) {
            com.sforce.ws.bind.XmlObject child = children.next();
            if (child != null && child.getName() != null) {
                String fieldName = child.getName().getLocalPart();
                Object fieldValue = child.getValue();
                if (fieldName != null && !"type".equals(fieldName)) {
                    fields.put(fieldName, fieldValue != null ? fieldValue.toString() : null);
                }
            }
        }
        
        // nullに設定されるフィールドも記録
        String[] nullFields = sObject.getFieldsToNull();
        if (nullFields != null) {
            for (String field : nullFields) {
                fields.put(field, null);
            }
        }
        
        return fields;
    }
    
    private String generateFilePath() {
        String basePath = outputPath;
        
        // ディレクトリパスの場合の処理
        if (basePath.endsWith("/") || basePath.endsWith("\\")) {
            // ディレクトリの場合、デフォルトファイル名を追加
            basePath = basePath + "sf_errors.jsonl";
        } else if (!basePath.contains(".")) {
            // 拡張子がない場合、ディレクトリとみなして処理
            if (!basePath.endsWith(java.io.File.separator)) {
                basePath = basePath + java.io.File.separator;
            }
            basePath = basePath + "sf_errors.jsonl";
        } else if (!basePath.endsWith(".jsonl")) {
            // 別の拡張子がある場合、.jsonlに変更
            basePath = basePath.substring(0, basePath.lastIndexOf(".")) + ".jsonl";
        }
        
        // タスクインデックスを含むファイル名を生成
        String base = basePath.substring(0, basePath.lastIndexOf(".jsonl"));
        return String.format("%s_task%03d.jsonl", base, taskIndex);
    }
    
    private void ensureDirectoryExists(Path path) throws IOException {
        Path parent = path.getParent();
        if (parent != null && !Files.exists(parent)) {
            Files.createDirectories(parent);
        }
    }
    
    
    @Override
    public void close() {
        if (writer != null) {
            try {
                writer.flush();
                writer.close();
                writer = null;  // 複数回のclose呼び出しを防ぐ
                
                if (errorCount == 0 && filePath != null) {
                    Files.deleteIfExists(filePath);
                }
            } catch (IOException e) {
                logger.error("Failed to close error file", e);
            }
        }
    }
}