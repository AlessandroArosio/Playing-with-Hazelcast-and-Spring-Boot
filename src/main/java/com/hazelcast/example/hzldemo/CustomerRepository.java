package com.hazelcast.example.hzldemo;

import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;

import java.util.List;

public interface CustomerRepository extends CrudRepository<Customer, Long> {

    @Query("select c from Customer c")
    List<Customer> findAll(Pageable pageable);


    @Query("select count(c.id) from Customer c")
    int findCount();
}
