package com.hazelcast.example.hzldemo;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.core.ILock;
import com.hazelcast.core.IMap;
import com.hazelcast.cp.CPSubsystem;
import com.hazelcast.cp.lock.FencedLock;
import com.hazelcast.query.SqlPredicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;

@RestController
public class Controller {

  private static Logger LOGGER = LoggerFactory.getLogger(Controller.class);

  @Autowired
  @Qualifier("nameMap")
  private IMap<String, String> nameMap;

  @Autowired
  @Qualifier("customerMap")
  private IMap<Long, Customer> customerMap;

  @Autowired
  private Service service;

  @Autowired
  @Qualifier("hzl")
  private HazelcastInstance hazelcastInstance;

  @RequestMapping("/putValue")
  public String putValue(@RequestParam("key") String key,
                         @RequestParam("value") String value) {
    nameMap.put(key, value);
    return "ok";
  }

  @RequestMapping("/getValue")
  public String getValue(@RequestParam("key") String key) {
    return nameMap.get(key);
  }

  @RequestMapping("/loadData")
  public String loadData() {
    Map<Long, Customer> tempMap = new HashMap<>();
    int count = service.count();
    final int BATCH_SIZE = 1000;
    int number_of_pages = count / BATCH_SIZE;

    for (int i = 0; i < number_of_pages; i++) {
      // processing in batch all the records from the database in the temporary map first - for better performances
      service.getCustomers(PageRequest.of(i, BATCH_SIZE)).forEach(e -> tempMap.put(e.getId(), e));

      // and saving in the customers map all the batch (atomically) in hazelcast
      customerMap.putAll(tempMap);
      // freeing up memory
      tempMap.clear();
      LOGGER.info("Loaded: {}", i);
    }
      LOGGER.info("Data loaded into hazelcast cluster");
    return "Data loaded into hazelcast cluster";
  }

  @RequestMapping("/getCustomerFromDB")
  public Customer getCustomerFromDB(@RequestParam("id") Long id) {
    return service.getCustomerById(id);
  }

  @RequestMapping("/getCustomerCache")
  public Customer getCustomerFromCache(@RequestParam("id") Long id) {
    return customerMap.get(id);
  }

  @RequestMapping("/query")
  public Collection<Customer> sqlPredicate(@RequestBody String query) {
    return customerMap.values(new SqlPredicate(query));
  }

  @RequestMapping("/lockExample")
  public String lockExample() throws InterruptedException {
    // this is deprecated, at some point change with CP Subsystem.
    ILock myLock = hazelcastInstance.getLock("myLock");
//    FencedLock myLock = hazelcastInstance.getCPSubsystem().getLock("myLock");
    try {
      LOGGER.info("Acquiring lock...");
      myLock.lock();
      LOGGER.info("Lock acquired!");
      Thread.sleep(10000);
      LOGGER.info("lockExample() executed successfully");
    } finally {
      myLock.unlock();
    }
    return ":)";
  }

  // distributing a task over nodes connected to hazelcast network
  @RequestMapping("/executorExample")
  public String executorExample() {
    IExecutorService exec = hazelcastInstance.getExecutorService("exec");
    IntStream.range(1, 1000).forEach(i -> {
      exec.submit(new EchoTask(i));
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    });
    return "executor!";
  }

}
