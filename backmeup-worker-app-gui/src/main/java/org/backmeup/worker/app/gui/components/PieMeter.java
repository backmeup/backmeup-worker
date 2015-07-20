package org.backmeup.worker.app.gui.components;

import java.awt.BasicStroke;
import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.Stroke;

@SuppressWarnings("serial")
public class PieMeter extends BaseMeter {
    private transient Stroke stroke = new BasicStroke(STROKE_WIDTH);

    public PieMeter() {

    }

    @Override
    public void paint(Graphics2D g, double percFilled, Color borderColor,
            Color foreColor, Color backColor) {
        if (percFilled >= 0 && percFilled <= 1) {
            int w = this.getWidth() - STROKE_WIDTH;
            int h = this.getHeight() - STROKE_WIDTH;
            int theta = (int) (360 * percFilled + 0.5);

            g.setStroke(stroke);
            g.setColor(backColor);
            g.fillOval(STROKE_WIDTH / 2, STROKE_WIDTH / 2, w, h);
            g.setColor(foreColor);
            g.fillArc(STROKE_WIDTH / 2, STROKE_WIDTH / 2, w, h, 90, -theta);
            g.setColor(borderColor);
            g.drawOval(STROKE_WIDTH / 2, STROKE_WIDTH / 2, w, h);
        }
    }
}
