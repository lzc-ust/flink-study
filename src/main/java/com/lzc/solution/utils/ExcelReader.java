package com.lzc.solution.utils;

import com.lzc.solution.objects.Customer;
import com.lzc.solution.objects.LineItem;
import com.lzc.solution.objects.Orders;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.util.IOUtils;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.FileInputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

public class ExcelReader {

    static {
        // Set the maximum allowable size for byte arrays
        IOUtils.setByteArrayMaxOverride(500_000_000); // 500 MB
    }

    public static List<LineItem> readLineItemsFromExcel(String excelFilePath) {
        List<LineItem> lineItems = new ArrayList<>();

        try (FileInputStream fis = new FileInputStream(excelFilePath);
             Workbook workbook = new XSSFWorkbook(fis)) {

            Sheet sheet = workbook.getSheetAt(0);
            for (Row row : sheet) {
                LineItem lineItem = new LineItem();
                lineItem.setL_orderkey((int) row.getCell(0).getNumericCellValue());
                lineItem.setL_partkey((int) row.getCell(1).getNumericCellValue());
                lineItem.setL_suppkey((int) row.getCell(2).getNumericCellValue());
                lineItem.setL_linenumber((int) row.getCell(3).getNumericCellValue());
                lineItem.setL_quantity(BigDecimal.valueOf(row.getCell(4).getNumericCellValue()));
                lineItem.setL_extendedprice(BigDecimal.valueOf(row.getCell(5).getNumericCellValue()));
                lineItem.setL_discount(BigDecimal.valueOf(row.getCell(6).getNumericCellValue()));
                lineItem.setL_tax(BigDecimal.valueOf(row.getCell(7).getNumericCellValue()));
                lineItem.setL_returnflag(row.getCell(8).getStringCellValue());
                lineItem.setL_linestatus(row.getCell(9).getStringCellValue());
                lineItem.setL_shipdate(row.getCell(10).getLocalDateTimeCellValue().toLocalDate());
                lineItem.setL_commitdate(row.getCell(11).getLocalDateTimeCellValue().toLocalDate());
                lineItem.setL_receiptdate(row.getCell(12).getLocalDateTimeCellValue().toLocalDate());
                lineItem.setL_shipinstruct(row.getCell(13).getStringCellValue());
                lineItem.setL_shipmode(row.getCell(14).getStringCellValue());
                lineItem.setL_comment(row.getCell(15).getStringCellValue());
                lineItems.add(lineItem);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return lineItems;
    }

    public static List<Orders> readOrdersFromExcel(String excelFilePath) {
        List<Orders> ordersList = new ArrayList<>();

        try (FileInputStream fis = new FileInputStream(excelFilePath);
             Workbook workbook = new XSSFWorkbook(fis)) {

            Sheet sheet = workbook.getSheetAt(0);
            for (Row row : sheet) {
                Orders order = new Orders();
                order.setO_orderkey((int) row.getCell(0).getNumericCellValue());
                order.setO_custkey((int) row.getCell(1).getNumericCellValue());
                order.setO_orderstatus(row.getCell(2).getStringCellValue());
                order.setO_totalprice(BigDecimal.valueOf(row.getCell(3).getNumericCellValue()));
                order.setO_orderdate(row.getCell(4).getLocalDateTimeCellValue().toLocalDate());
                order.setO_orderpriority(row.getCell(5).getStringCellValue());
                order.setO_clerk(row.getCell(6).getStringCellValue());
                order.setO_shippriority((int) row.getCell(7).getNumericCellValue());
                order.setO_comment(row.getCell(8).getStringCellValue());
                ordersList.add(order);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return ordersList;
    }

    public static List<Customer> readCustomersFromExcel(String excelFilePath) {
        List<Customer> customers = new ArrayList<>();

        try (FileInputStream fis = new FileInputStream(excelFilePath);
             Workbook workbook = new XSSFWorkbook(fis)) {

            Sheet sheet = workbook.getSheetAt(0);
            for (Row row : sheet) {
                Customer customer = new Customer();
                customer.setC_custkey((int) row.getCell(0).getNumericCellValue());
                customer.setC_name(row.getCell(1).getStringCellValue());
                customer.setC_address(row.getCell(2).getStringCellValue());
                customer.setC_nationkey((int) row.getCell(3).getNumericCellValue());
                customer.setC_phone(row.getCell(4).getStringCellValue());
                customer.setC_acctbal(BigDecimal.valueOf(row.getCell(5).getNumericCellValue()));
                customer.setC_mktsegment(row.getCell(6).getStringCellValue());
                customer.setC_comment(row.getCell(7).getStringCellValue());
                customers.add(customer);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return customers;
    }
}
