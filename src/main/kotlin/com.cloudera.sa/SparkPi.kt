package com.cloudera.sa.kotlin.SparkPi

import scala.math

import org.apache
import org.apache.spark.*


public fun getHelloString() : String {
    return "Hello, world!"
}

/** Computes an approximation to pi */
fun main(args : Array<String>) {
    if (args.size() == 0) {
        println("Usage: SparkPi <master> [<slices>]")
    }

    // Process Args
    val conf = SparkConf()
    .setMaster(args[0])
    //.setAppName(javaClass<com.cloudera.sa.kotlin.SparkPi.SparkPiPackage>().getCanonicalName())
    //.setJars()

    val spark = SparkContext(conf)
    val slices = if (args.size() > 1) args[1].toInt() else 2
    val n = 100000 * slices

    // Run spark job
    val count = spark.parallelize(1 to n, slices).map {
        val x = scala.math.random() * 2 - 1
        val y = scala.math.random() * 2 - 1
        if (x*x + y*y < 1) 1 else 0
    }.reduce{a, b -> a + b}

    // Output & Close
    println("Pi is roughly " + 4.0 * count / n)
    spark.stop()
}