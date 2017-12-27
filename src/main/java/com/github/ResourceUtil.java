package com.github;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class ResourceUtil {

    public static byte[] getResource(String resourceName) throws IOException {
        String resource = ResourceUtil.class.getClassLoader().getResource(resourceName).getPath();
        Path resourcePath = Paths.get( resource );
        return Files.readAllBytes( resourcePath );
    }

}
