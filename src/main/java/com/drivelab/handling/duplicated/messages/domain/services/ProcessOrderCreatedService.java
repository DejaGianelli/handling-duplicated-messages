package com.drivelab.handling.duplicated.messages.domain.services;

import com.drivelab.handling.duplicated.messages.domain.entities.Ticket;
import com.drivelab.handling.duplicated.messages.domain.entities.TicketRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class ProcessOrderCreatedService {

    private final TicketRepository orderRepository;

    @Autowired
    public ProcessOrderCreatedService(TicketRepository orderRepository) {
        this.orderRepository = orderRepository;
    }

    public void process(String dishName, Double price) {
        Ticket ticket = new Ticket(dishName, price);
        orderRepository.save(ticket);
    }
}
