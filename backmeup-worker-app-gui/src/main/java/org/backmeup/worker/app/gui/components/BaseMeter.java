package org.backmeup.worker.app.gui.components;

import java.awt.Color;
import java.awt.Graphics;
import java.awt.Graphics2D;

import javax.swing.JComponent;

@SuppressWarnings("serial")
public abstract class BaseMeter extends JComponent {
    protected static final int STROKE_WIDTH = 1;
    
    private int min = 0;
    private int max = 100;
    private int value = min;
    private Color backColor = Color.white;
    private Color foreColor = Color.red;
    private Color borderColor = Color.blue;

    public BaseMeter() {

    }

    public void setMinimum(int min) {
        this.min = min;
        repaint();
    }

    public int getMinimum() {
        return min;
    }

    public void setMaximum(int max) {
        this.max = max;
        repaint();
    }

    public int getMaximum() {
        return max;
    }

    public void setValue(int val) {
        if (min <= val && val <= max) {
            this.value = val;
            repaint();
        }
    }

    public int getValue() {
        return value;
    }

    public void setBackgroundColor(Color col) {
        this.backColor = col;
        repaint();
    }

    public Color getBackgroundColor() {
        return this.backColor;
    }

    public void setForegroundColor(Color col) {
        this.foreColor = col;
        repaint();
    }

    public Color getForegroundColor() {
        return this.foreColor;
    }

    public void setBorderColor(Color col) {
        this.borderColor = col;
        repaint();
    }

    public Color getBorderColor() {
        return this.borderColor;
    }

    @Override
    public void paintComponent(Graphics g) {
        Graphics2D g2 = (Graphics2D) g;
        paint(g2, (double) value / (max - min), borderColor, foreColor,
                backColor);
    }

    public abstract void paint(Graphics2D g, double percFilled,
            Color borderColor, Color foreColor, Color backColor);
}
