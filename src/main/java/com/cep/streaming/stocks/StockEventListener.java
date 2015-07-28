package com.cep.streaming.stocks;

import com.cep.streaming.listener.EventListener;
import com.cep.streaming.listener.MapEvent;

/**
 * Created by jayant on 7/20/15.
 */
public class StockEventListener extends EventListener {

    public void onEvent(MapEvent event) {
        System.out.println("RECEIVED EVENT");
    }
}
