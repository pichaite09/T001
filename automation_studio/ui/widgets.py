from __future__ import annotations

from PySide6 import QtWidgets


class CardFrame(QtWidgets.QFrame):
    def __init__(self, parent: QtWidgets.QWidget | None = None) -> None:
        super().__init__(parent)
        self.setProperty("card", True)
        self.setFrameShape(QtWidgets.QFrame.Shape.NoFrame)


def make_button(text: str, variant: str | None = None) -> QtWidgets.QPushButton:
    button = QtWidgets.QPushButton(text)
    if variant:
        button.setProperty("variant", variant)
        button.style().unpolish(button)
        button.style().polish(button)
    return button


def make_form_label(text: str) -> QtWidgets.QLabel:
    label = QtWidgets.QLabel(text)
    label.setObjectName("subtitleLabel")
    return label
