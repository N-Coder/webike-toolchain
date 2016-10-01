from gi.repository import Gtk
from matplotlib.backends.backend_gtk3 import NavigationToolbar2GTK3 as MatplotlibToolbar, Toolbar


class BaseToolbar(Toolbar):
    def insert_widget(self, widget, tooltip):
        toolitem = Gtk.ToolItem()
        toolitem.add(widget)
        toolitem.set_tooltip_text(tooltip)
        self.insert(toolitem, -1)
        return widget

    def insert_button(self, text, tooltip_text, icon, callback):
        image = Gtk.Image()
        image.set_from_stock(icon, self.get_icon_size())
        tbutton = Gtk.ToolButton()
        tbutton.set_label(text)
        tbutton.set_icon_widget(image)
        self.insert(tbutton, -1)
        tbutton.connect('clicked', callback)
        tbutton.set_tooltip_text(tooltip_text)
        return tbutton

    def insert_separator(self):
        widget = Gtk.SeparatorToolItem()
        self.insert(widget, -1)
        return widget


class PlotToolbar(MatplotlibToolbar, BaseToolbar):
    def _init_toolbar(self):
        self.set_style(Gtk.ToolbarStyle.BOTH_HORIZ)

        # matplotlib buttons
        self.insert_button('Reset', 'Reset original view', Gtk.STOCK_ZOOM_100, self.home).set_is_important(True)
        self.insert_button('Back', 'Back to  previous view', Gtk.STOCK_UNDO, self.back)
        self.insert_button('Forward', 'Forward to next view', Gtk.STOCK_REDO, self.forward)
        self.insert_button('Pan', 'Pan axes with left mouse, zoom with right', Gtk.STOCK_ZOOM_IN,
                           self.pan).set_is_important(True)
        self.insert_button('Zoom', 'Zoom to rectangle', Gtk.STOCK_ZOOM_FIT, self.zoom).set_is_important(True)
        self.insert_separator()
        self.insert_button('Toggle Legend', 'Show/hide legend', Gtk.STOCK_HELP, self.toggle_legend)
        self.insert_button('Layout', 'Fit subplot layout to window', Gtk.STOCK_FULLSCREEN, self.pack)
        self.insert_button('Subplots', 'Configure subplots', Gtk.STOCK_PREFERENCES, self.configure_subplots)
        self.insert_separator()
        self.insert_button('Save', 'Save the figure', Gtk.STOCK_SAVE, self.save_figure).set_is_important(True)

        # matplotlib status
        toolitem = Gtk.SeparatorToolItem()
        toolitem.set_draw(False)
        toolitem.set_expand(True)
        self.insert(toolitem, -1)

        self.message = Gtk.Label()
        toolitem = Gtk.ToolItem()
        toolitem.add(self.message)
        self.insert(toolitem, -1)

    def toggle_legend(self, widget):
        legend = self.canvas.figure.add_subplot(111).legend_
        legend.set_visible(not legend.get_visible())
        self.canvas.draw()

    def pack(self, widget):
        self.canvas.figure.tight_layout()
        self.canvas.draw()
