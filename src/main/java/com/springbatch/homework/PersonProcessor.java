package com.springbatch.homework;

import org.springframework.batch.item.ItemProcessor;

import java.time.LocalDate;
import java.time.Period;

public class PersonProcessor implements ItemProcessor<Person, Person> {
    @Override
    public Person process(Person item) throws Exception {
        LocalDate birth = LocalDate.parse(item.getBirthDate());

        Period period = Period.between(birth, LocalDate.now());
        return new Person(item.getFirstName(), item.getLastName(),
                item.getBirthDate(), period.getYears());
    }
}
