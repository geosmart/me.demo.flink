package me.demo.flink.source

import java.sql.Timestamp

open class Event {
    lateinit var user: String
    lateinit var url: String
    var timestamp: Long = 0

    constructor()

    constructor(user: String, url: String, timestamp: Long) {
        this.user = user
        this.url = url
        this.timestamp = timestamp
    }

    override fun toString(): String {
        return "Event(user='$user', url='$url', timestamp=${timestamp}，${Timestamp(timestamp)})"
    }

 }
