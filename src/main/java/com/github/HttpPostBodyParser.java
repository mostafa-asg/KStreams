package com.github;

import java.util.HashMap;
import java.util.Map;

public class HttpPostBodyParser {

    private Map<String,String> formData = new HashMap<>();

    public HttpPostBodyParser(String body) {

        String[] data = body.split("&");

        for (String datum : data) {

            String[] keyValue = datum.split("=");

            formData.put( keyValue[0] , keyValue[1] );

        }

    }

    public String get(String formElement) {
        return formData.get(formElement);
    }
}
