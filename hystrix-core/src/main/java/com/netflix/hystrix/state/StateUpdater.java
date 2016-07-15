package com.netflix.hystrix.state;

import com.netflix.hystrix.CommandDataStyle;

public abstract class StateUpdater<R> {
    public static <R> StateUpdater<R> from(CommandDataStyle commandDataStyle) {
        switch (commandDataStyle) {
            case SCALAR: return new ScalarStateUpdater();
            case MULTIVALUED: return new StreamingStateUpdater();
            default: throw new IllegalArgumentException("Unknown CommandDataStyle : " + commandDataStyle);
        }
    }
}
