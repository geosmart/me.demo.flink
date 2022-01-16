package me.demo.flink.report;

public class UnimplementedException extends RuntimeException {
    
    public UnimplementedException() {
        super("This method has not yet been implemented");
    }
}