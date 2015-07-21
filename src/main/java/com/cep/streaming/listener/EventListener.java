package com.cep.streaming.listener;

/**
 * Created by jayant on 7/20/15.
 */
public abstract class EventListener {

    public abstract void onEvent(MapEvent event);

    public static EventListener createListener(Class c) throws Exception {
        Object ob = c.newInstance();

        if (ob instanceof EventListener == false) {
            throw new Exception("Listener not of type EventListener");
        }

        EventListener eventListener = (EventListener)ob;

        return eventListener;

    }
}
