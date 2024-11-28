package com.lzc.solution.process;

import com.lzc.solution.dao.ResultDao;
import com.lzc.solution.objects.Result;
import org.apache.ibatis.io.Resources;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;

import java.io.IOException;
import java.io.Reader;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class ResultVerification {
    public static void main(String[] args) throws IOException {
        String resource = "mybatis-config.xml";
        Reader reader = Resources.getResourceAsReader(resource);
        SqlSessionFactory sqlSessionFactory = new SqlSessionFactoryBuilder().build(reader);

        try (SqlSession session = sqlSessionFactory.openSession()) {
            ResultDao mapper = session.getMapper(ResultDao.class);

            List<Result> results = mapper.queryOrdersRevenue("1995-03-13", "1995-03-13");

            List<Result> flinkResult = mapper.queryFlinkResult();

            Set<String> set = new HashSet<>();
            for (Result result : results) {
                String key = getKey(result);
                set.add(key);
            }

            Set<String> flinkSet = new HashSet<>();
            for (Result result : flinkResult) {
                String key = getKey(result);
                flinkSet.add(key);
                if (!set.contains(key)) {
                    System.out.println("Flink processing results are wrong:" + key);
                }
            }

            // List<String> miss = set.stream().filter(key -> set.contains(key) && !flinkSet.contains(key)).collect(Collectors.toList());
            // System.out.println(miss);
            System.out.println("Flink processing results are correct!");
        }
    }

    private static String getKey(Result result) {
        return result.getL_orderkey() + " " + result.getRevenue() + " " + result.getO_orderdate() +
                " " + result.getO_shippriority();
    }
}
