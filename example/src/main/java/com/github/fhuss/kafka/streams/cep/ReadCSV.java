/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.fhuss.kafka.streams.cep;

import com.opencsv.CSVReader;
import com.opencsv.bean.CsvToBean;
import com.opencsv.bean.CsvToBeanBuilder;

import java.io.FileReader;
import java.util.List;

public class ReadCSV {
    private String csvFileName;
    private List datapoints;

    ReadCSV(String csvFileName){
        this.csvFileName = csvFileName;
    }

    public List ReadCSVFile(){
        try {
            CSVReader csvReader = new CSVReader(new FileReader(csvFileName));
            CsvToBean csvToBean = new CsvToBeanBuilder(csvReader)
                    .withType(QnVEvent.class)
                    .withIgnoreLeadingWhiteSpace(true)
                    .build();

            datapoints = csvToBean.parse();

        }catch (Exception e){
            System.out.println("File not found...");
        }
        return datapoints;
    }
}
