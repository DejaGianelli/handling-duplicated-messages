package com.drivelab.handling.duplicated.messages.domain.entities;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;

import java.time.LocalDateTime;

@Entity
@Table(name = "tickets")
@Getter
@Setter
public class Ticket {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Integer id;

    private String dishName;

    private Double price;

    private LocalDateTime createdAt;

    public Ticket(String dishName, Double price) {
        this();
        this.dishName = dishName;
        this.price = price;
    }

    public Ticket() {
        this.createdAt = LocalDateTime.now();
    }
}
