package com.spark.project.common;

import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;
import org.springframework.web.multipart.MultipartFile;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
public class HdfsClient {
    private static final int bufferSize = 1024 * 1024 * 64;

    @Resource
    private FileSystem fileSystem;

    public boolean makeFolder(String path) {
        boolean target = false;
        if (!StringUtils.hasText(path)) {
            return false;
        }
        if (existFile(path)) {
            return true;
        }
        Path src = new Path(path);
        try {
            target = fileSystem.mkdirs(src);
        } catch (IOException e) {
            log.error(e.getMessage());
        }
        return target;
    }

    public boolean existFile(String path) {
        if (!StringUtils.hasText(path)) {
            return false;
        }
        Path src = new Path(path);
        try {
            return fileSystem.exists(src);
        } catch (IOException e) {
            log.error(e.getMessage());
        }
        return false;
    }

    public List<Map<String, Object>> readCatalog(String path) {
        if (!StringUtils.hasText(path)) {
            return Collections.emptyList();
        }
        if (!existFile(path)) {
            log.error("catalog is not exist!!");
            return Collections.emptyList();
        }

        Path src = new Path(path);
        FileStatus[] fileStatuses = null;
        try {
            fileStatuses = fileSystem.listStatus(src);
        } catch (IOException e) {
            log.error(e.getMessage());
        }
        List<Map<String, Object>> result = new ArrayList<>(fileStatuses.length);

        if (null != fileStatuses && 0 < fileStatuses.length) {
            for (FileStatus fileStatus : fileStatuses) {
                Map<String, Object> cataLogMap = Maps.newHashMap();
                cataLogMap.put("filePath", fileStatus.getPath());
                cataLogMap.put("fileStatus", fileStatus);
                result.add(cataLogMap);
            }
        }
        return result;
    }

    public boolean createFile(String path, MultipartFile file) {
        boolean target = false;
        if (StringUtils.isEmpty(path)) {
            return false;
        }
        String fileName = file.getName();
        Path newPath = new Path(path + "/" + fileName);

        FSDataOutputStream outputStream = null;
        try {
            outputStream = fileSystem.create(newPath);
            outputStream.write(file.getBytes());
            target = true;
        } catch (IOException e) {
            log.error(e.getMessage());
        } finally {
            if (null != outputStream) {
                try {
                    outputStream.close();
                } catch (IOException e) {
                    log.error(e.getMessage());
                }
            }
        }
        return target;
    }

    public String readFileContent(String path) {
        if (StringUtils.isEmpty(path)) {
            return null;
        }

        if (!existFile(path)) {
            return null;
        }

        Path src = new Path(path);

        FSDataInputStream inputStream = null;
        StringBuilder sb = new StringBuilder();
        try {
            inputStream = fileSystem.open(src);
            String lineText = "";
            while ((lineText = inputStream.readLine()) != null) {
                sb.append(lineText);
            }
        } catch (IOException e) {
            log.error(e.getMessage());
        } finally {
            if (null != inputStream) {
                try {
                    inputStream.close();
                } catch (IOException e) {
                    log.error(e.getMessage());
                }
            }
        }
        return sb.toString();
    }

    public List<Map<String, Object>> listFile(String path) {
        if (StringUtils.isEmpty(path)) {
            return Collections.emptyList();
        }
        if (!existFile(path)) {
            return Collections.emptyList();
        }
        List<Map<String, Object>> resultList = new ArrayList<>();

        Path src = new Path(path);
        try {
            RemoteIterator<LocatedFileStatus> fileIterator = fileSystem.listFiles(src, true);
            while (fileIterator.hasNext()) {
                LocatedFileStatus next = fileIterator.next();
                Path filePath = next.getPath();
                String fileName = filePath.getName();
                Map<String, Object> map = Maps.newHashMap();
                map.put("fileName", fileName);
                map.put("filePath", filePath.toString());
                resultList.add(map);
            }
        } catch (IOException e) {
            log.error(e.getMessage());
        }

        return resultList;
    }

    public boolean renameFile(String oldName, String newName) {
        boolean target = false;
        if (StringUtils.isEmpty(oldName) || StringUtils.isEmpty(newName)) {
            return false;
        }
        Path oldPath = new Path(oldName);
        Path newPath = new Path(newName);
        try {
            target = fileSystem.rename(oldPath, newPath);
        } catch (IOException e) {
            log.error(e.getMessage());
        }

        return target;
    }

    public boolean deleteFile(String path) {
        boolean target = false;
        if (StringUtils.isEmpty(path)) {
            return false;
        }
        if (!existFile(path)) {
            return false;
        }
        Path src = new Path(path);
        try {
            target = fileSystem.deleteOnExit(src);
        } catch (IOException e) {
            log.error(e.getMessage());
        }
        return target;
    }

    public boolean uploadFile(String path, String uploadPath) {
        if (StringUtils.isEmpty(path) || StringUtils.isEmpty(uploadPath)) {
            return false;
        }

        Path clientPath = new Path(path);
        Path serverPath = new Path(uploadPath);

        try {
            fileSystem.copyFromLocalFile(false, clientPath, serverPath);
            return true;
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }
        return false;
    }

    public boolean downloadFile(String path, String downloadPath) {
        if (StringUtils.isEmpty(path) || StringUtils.isEmpty(downloadPath)) {
            return false;
        }

        Path clienPath = new Path(path);
        Path targetPath = new Path(downloadPath);

        try {
            fileSystem.copyToLocalFile(false, clienPath, targetPath);
            return true;
        } catch (IOException e) {
            log.error(e.getMessage());
        }
        return false;
    }

    public boolean copyFile(String sourcePath, String targetPath) {
        if (StringUtils.isEmpty(sourcePath) || StringUtils.isEmpty(targetPath)) {
            return false;
        }

        Path oldPath = new Path(sourcePath);
        Path newPath = new Path(targetPath);

        FSDataInputStream inputStream = null;
        FSDataOutputStream outputStream = null;

        try {
            inputStream = fileSystem.open(oldPath);
            outputStream = fileSystem.create(newPath);
            IOUtils.copyBytes(inputStream, outputStream, bufferSize, false);
            return true;
        } catch (IOException e) {
            log.error(e.getMessage());
        } finally {
            if (null != inputStream) {
                try {
                    inputStream.close();
                } catch (IOException e) {
                    log.error(e.getMessage());
                }
            }
            if (null != outputStream) {
                try {
                    outputStream.close();
                } catch (IOException e) {
                    log.error(e.getMessage());
                }
            }
        }
        return false;
    }

    public byte[] openFileToBytes(String path) {

        if (StringUtils.isEmpty(path)) {
            return null;
        }

        if (!existFile(path)) {
            return null;
        }

        Path src = new Path(path);
        byte[] result = null;
        FSDataInputStream inputStream = null;
        try {
            inputStream = fileSystem.open(src);
            result = IOUtils.readFullyToByteArray(inputStream);
        } catch (IOException e) {
            log.error(e.getMessage());
        } finally {
            if (null != inputStream) {
                try {
                    inputStream.close();
                } catch (IOException e) {
                    log.error(e.getMessage());
                }
            }
        }

        return result;
    }

    public BlockLocation[] getFileBlockLocations(String path) {
        if (StringUtils.isEmpty(path)) {
            return null;
        }
        if (!existFile(path)) {
            return null;
        }
        BlockLocation[] blocks = null;
        Path src = new Path(path);
        try {
            FileStatus fileStatus = fileSystem.getFileStatus(src);
            blocks = fileSystem.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
        } catch (Exception e) {
            log.error(e.getMessage());
        }
        return blocks;
    }
}
