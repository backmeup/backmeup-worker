package org.backmeup.worker.app.gui.components;

import java.awt.BasicStroke;
import java.awt.Color;
import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.Stroke;

import javax.swing.JComponent;

@SuppressWarnings("serial")
public class PieMeter extends JComponent {
	protected static final int strokeWidth = 1;

	private int min = 0;
	private int max = 100;
	private int value = min;
	private Color backColor = Color.white;
	private Color foreColor = Color.red;
	private Color borderColor = Color.blue;
	
	private Stroke stroke = new BasicStroke(strokeWidth);

	public PieMeter() {
		
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

	public void paintComponent(Graphics g) {
		Graphics2D g2 = (Graphics2D) g;
		paintDisc(g2, (double) value / (max - min), borderColor, foreColor, backColor);
	}

	private void paintDisc(Graphics2D g, double percFilled, Color borderColor, Color foreColor, Color backColor) {
		if (percFilled >= 0 && percFilled <= 1) {
			int w = this.getWidth() - strokeWidth;
			int h = this.getHeight() - strokeWidth;
			int theta = (int) (360 * percFilled + 0.5);

			g.setStroke(stroke);
			g.setColor(backColor);
			g.fillOval(strokeWidth / 2, strokeWidth / 2, w, h);
			g.setColor(foreColor);
			g.fillArc(strokeWidth / 2, strokeWidth / 2, w, h, 90, -theta);
			g.setColor(borderColor);
			g.drawOval(strokeWidth / 2, strokeWidth / 2, w, h);
		}
	}
}
