APP_STYLESHEET = """
QWidget {
    background: #0b1220;
    color: #e5edf7;
    font-family: "Segoe UI";
    font-size: 10pt;
}

QMainWindow, QFrame#rootFrame {
    background: #09111d;
}

QLabel#titleLabel {
    font-size: 20pt;
    font-weight: 700;
    color: #f8fbff;
}

QLabel#subtitleLabel {
    color: #92a3b8;
    font-size: 10pt;
}

QFrame[card="true"] {
    background: qlineargradient(x1:0, y1:0, x2:1, y2:1,
        stop:0 #111b2d, stop:1 #0d1626);
    border: 1px solid #223249;
    border-radius: 18px;
}

QFrame[navigation="true"] {
    background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
        stop:0 #0f1728, stop:1 #0b1220);
    border-right: 1px solid #1f2d40;
}

QListWidget#navList {
    background: transparent;
    border: none;
    outline: none;
    padding: 10px 8px;
}

QListWidget#navList::item {
    margin: 6px 0;
    padding: 12px 14px;
    border-radius: 12px;
    color: #aebfd3;
}

QListWidget#navList::item:selected {
    background: #19314d;
    color: #f8fbff;
}

QPushButton {
    background: #1b76f2;
    color: white;
    border: none;
    border-radius: 10px;
    padding: 9px 14px;
    font-weight: 600;
}

QPushButton:hover {
    background: #3290ff;
}

QPushButton:disabled {
    background: #39506f;
    color: #99a9bc;
}

QPushButton[variant="secondary"] {
    background: #18263a;
    border: 1px solid #2a3d58;
    color: #dbe6f3;
}

QPushButton[variant="danger"] {
    background: #a63c4d;
}

QLineEdit, QTextEdit, QPlainTextEdit, QComboBox, QSpinBox, QTableWidget, QListWidget, QCheckBox {
    background: #0c1625;
    border: 1px solid #2a3b50;
    border-radius: 10px;
    selection-background-color: #215597;
}

QLineEdit, QComboBox, QSpinBox {
    min-height: 18px;
    padding: 8px 10px;
}

QTextEdit, QPlainTextEdit {
    padding: 8px;
}

QHeaderView::section {
    background: #132034;
    color: #dce7f3;
    padding: 8px;
    border: none;
    border-bottom: 1px solid #29415e;
}

QTableWidget {
    gridline-color: #223248;
    background: #0c1625;
    alternate-background-color: #101d2f;
    color: #e5edf7;
    selection-background-color: #215597;
    selection-color: #ffffff;
}

QTableWidget::item {
    padding: 6px;
    background: transparent;
    color: #e5edf7;
}

QTableView {
    alternate-background-color: #101d2f;
    background: #0c1625;
    color: #e5edf7;
    selection-background-color: #215597;
    selection-color: #ffffff;
}

QScrollBar:vertical {
    background: transparent;
    width: 12px;
}

QScrollBar::handle:vertical {
    background: #274360;
    min-height: 30px;
    border-radius: 6px;
}

QSplitter::handle {
    background: #1e2d42;
    width: 1px;
    height: 1px;
}
"""
