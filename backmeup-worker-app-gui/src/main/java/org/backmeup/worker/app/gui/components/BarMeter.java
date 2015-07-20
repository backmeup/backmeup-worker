package org.backmeup.worker.app.gui.components;

import java.awt.BasicStroke;
import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.Stroke;

@SuppressWarnings("serial")
public class BarMeter extends BaseMeter {
    private transient Stroke stroke = new BasicStroke(STROKE_WIDTH);

    public BarMeter() {

    }

    @Override
    public void paint(Graphics2D g, double percFilled, Color borderColor,
            Color foreColor, Color backColor) {
        if (percFilled >= 0 && percFilled <= 1) {
            int w = this.getWidth() - STROKE_WIDTH;
            int h = this.getHeight() - STROKE_WIDTH;
            int val = (int) (w * percFilled);

            g.setStroke(stroke);
            g.setColor(backColor);
            g.fillRect(STROKE_WIDTH / 2, STROKE_WIDTH / 2, w, h);
            g.setColor(foreColor);
            g.fillRect(STROKE_WIDTH / 2, STROKE_WIDTH / 2, val, h);
            g.setColor(borderColor);
            g.drawRect(STROKE_WIDTH / 2, STROKE_WIDTH / 2, w, h);
        }
    }
}
