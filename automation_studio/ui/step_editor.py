from __future__ import annotations

import json

from PySide6 import QtCore, QtGui, QtWidgets

from automation_studio.models import (
    EXECUTION_POLICY_FIELDS,
    FLOW_CONTROL_FIELDS,
    STEP_DEFINITIONS,
    StepField,
    default_flow_control,
    default_execution_policy,
    definition_for,
    preset_map_for,
    validate_step_parameters,
)
from automation_studio.ui.widgets import CardFrame, make_button, make_form_label


class StepEditorDialog(QtWidgets.QDialog):
    def __init__(
        self,
        parent: QtWidgets.QWidget | None = None,
        step_data: dict | None = None,
        default_position: int = 1,
        workflow_choices: list[tuple[int, str]] | None = None,
    ) -> None:
        super().__init__(parent)
        self.step_data = step_data
        self.workflow_choices = list(workflow_choices or [])
        self.field_widgets: dict[str, QtWidgets.QWidget] = {}
        self.field_definitions: dict[str, StepField] = {}
        self.policy_widgets: dict[str, QtWidgets.QWidget] = {}
        self.flow_widgets: dict[str, QtWidgets.QWidget] = {}
        self.extra_parameters: dict[str, object] = {}
        self._is_loading = False
        self._applied_screen_constraints = False
        self.setWindowTitle("Step Editor")
        self.setModal(True)
        self._build_ui()
        self._load_data(default_position)
        self._apply_screen_constraints()

    def _build_ui(self) -> None:
        outer_layout = QtWidgets.QVBoxLayout(self)
        outer_layout.setContentsMargins(18, 18, 18, 18)
        outer_layout.setSpacing(12)

        self.scroll_area = QtWidgets.QScrollArea()
        self.scroll_area.setWidgetResizable(True)
        self.scroll_area.setFrameShape(QtWidgets.QFrame.Shape.NoFrame)
        self.scroll_area.setHorizontalScrollBarPolicy(QtCore.Qt.ScrollBarPolicy.ScrollBarAsNeeded)
        self.scroll_area.setVerticalScrollBarPolicy(QtCore.Qt.ScrollBarPolicy.ScrollBarAsNeeded)

        self.content_widget = QtWidgets.QWidget()
        root_layout = QtWidgets.QVBoxLayout(self.content_widget)
        root_layout.setContentsMargins(0, 0, 0, 0)
        root_layout.setSpacing(12)

        title = QtWidgets.QLabel("Step Editor")
        title.setObjectName("titleLabel")
        subtitle = QtWidgets.QLabel("แก้ไข step ผ่านฟอร์มเฉพาะชนิด step พร้อม preset, validation และ preview")
        subtitle.setObjectName("subtitleLabel")
        root_layout.addWidget(title)
        root_layout.addWidget(subtitle)

        general_card = CardFrame()
        general_layout = QtWidgets.QGridLayout(general_card)
        general_layout.setContentsMargins(18, 18, 18, 18)
        general_layout.setHorizontalSpacing(12)
        general_layout.setVerticalSpacing(10)

        general_layout.addWidget(make_form_label("Position"), 0, 0)
        self.step_position_input = QtWidgets.QSpinBox()
        self.step_position_input.setMinimum(1)
        self.step_position_input.setMaximum(999)
        general_layout.addWidget(self.step_position_input, 0, 1)

        general_layout.addWidget(make_form_label("Name"), 0, 2)
        self.step_name_input = QtWidgets.QLineEdit()
        general_layout.addWidget(self.step_name_input, 0, 3)

        general_layout.addWidget(make_form_label("Step Type"), 1, 0)
        self.step_type_combo = QtWidgets.QComboBox()
        for definition in STEP_DEFINITIONS:
            self.step_type_combo.addItem(f"{definition.label} ({definition.key})", definition.key)
        general_layout.addWidget(self.step_type_combo, 1, 1)

        self.step_enabled_check = QtWidgets.QCheckBox("Enabled")
        self.step_enabled_check.setChecked(True)
        general_layout.addWidget(self.step_enabled_check, 1, 2, 1, 2)
        root_layout.addWidget(general_card)

        preset_card = CardFrame()
        preset_layout = QtWidgets.QGridLayout(preset_card)
        preset_layout.setContentsMargins(18, 18, 18, 18)
        preset_layout.setHorizontalSpacing(12)
        preset_layout.setVerticalSpacing(10)

        preset_layout.addWidget(make_form_label("Preset"), 0, 0)
        self.preset_combo = QtWidgets.QComboBox()
        preset_layout.addWidget(self.preset_combo, 0, 1)
        self.apply_preset_button = make_button("Apply Preset", "secondary")
        self.apply_template_button = make_button("Apply Template", "secondary")
        preset_layout.addWidget(self.apply_preset_button, 0, 2)
        preset_layout.addWidget(self.apply_template_button, 0, 3)

        self.preset_description_label = QtWidgets.QLabel()
        self.preset_description_label.setObjectName("subtitleLabel")
        self.preset_description_label.setWordWrap(True)
        preset_layout.addWidget(self.preset_description_label, 1, 0, 1, 4)
        root_layout.addWidget(preset_card)

        policy_card = CardFrame()
        policy_layout = QtWidgets.QGridLayout(policy_card)
        policy_layout.setContentsMargins(18, 18, 18, 18)
        policy_layout.setHorizontalSpacing(12)
        policy_layout.setVerticalSpacing(10)

        policy_title = QtWidgets.QLabel("Execution Policy")
        policy_title.setObjectName("subtitleLabel")
        policy_layout.addWidget(policy_title, 0, 0, 1, 4)

        for index, field_definition in enumerate(EXECUTION_POLICY_FIELDS, start=1):
            row = 1 + ((index - 1) // 2) * 2
            column_pair = ((index - 1) % 2) * 2
            widget = self._create_widget_for_field(field_definition)
            self.policy_widgets[field_definition.key] = widget
            policy_layout.addWidget(make_form_label(field_definition.label), row, column_pair)
            policy_layout.addWidget(widget, row, column_pair + 1)
            self._connect_widget(widget)

        root_layout.addWidget(policy_card)

        flow_card = CardFrame()
        flow_layout = QtWidgets.QGridLayout(flow_card)
        flow_layout.setContentsMargins(18, 18, 18, 18)
        flow_layout.setHorizontalSpacing(12)
        flow_layout.setVerticalSpacing(10)

        flow_title = QtWidgets.QLabel("Flow Control")
        flow_title.setObjectName("subtitleLabel")
        flow_layout.addWidget(flow_title, 0, 0, 1, 4)

        for index, field_definition in enumerate(FLOW_CONTROL_FIELDS, start=1):
            row = 1 + ((index - 1) // 2) * 2
            column_pair = ((index - 1) % 2) * 2
            widget = self._create_widget_for_field(field_definition)
            self.flow_widgets[field_definition.key] = widget
            flow_layout.addWidget(make_form_label(field_definition.label), row, column_pair)
            flow_layout.addWidget(widget, row, column_pair + 1)
            self._connect_widget(widget)

        root_layout.addWidget(flow_card)

        content_splitter = QtWidgets.QSplitter()
        content_splitter.setChildrenCollapsible(False)

        form_card = CardFrame()
        form_layout = QtWidgets.QVBoxLayout(form_card)
        form_layout.setContentsMargins(18, 18, 18, 18)
        form_layout.setSpacing(10)
        form_title = QtWidgets.QLabel("Field Editor")
        form_title.setObjectName("subtitleLabel")
        form_layout.addWidget(form_title)

        self.form_scroll = QtWidgets.QScrollArea()
        self.form_scroll.setWidgetResizable(True)
        self.form_scroll.setFrameShape(QtWidgets.QFrame.Shape.NoFrame)
        self.form_container = QtWidgets.QWidget()
        self.form_fields_layout = QtWidgets.QFormLayout(self.form_container)
        self.form_fields_layout.setFieldGrowthPolicy(QtWidgets.QFormLayout.FieldGrowthPolicy.ExpandingFieldsGrow)
        self.form_fields_layout.setHorizontalSpacing(16)
        self.form_fields_layout.setVerticalSpacing(10)
        self.form_scroll.setWidget(self.form_container)
        form_layout.addWidget(self.form_scroll, 1)
        content_splitter.addWidget(form_card)

        preview_card = CardFrame()
        preview_layout = QtWidgets.QVBoxLayout(preview_card)
        preview_layout.setContentsMargins(18, 18, 18, 18)
        preview_layout.setSpacing(10)
        preview_title = QtWidgets.QLabel("JSON Preview")
        preview_title.setObjectName("subtitleLabel")
        preview_layout.addWidget(preview_title)

        self.step_hint_label = QtWidgets.QLabel()
        self.step_hint_label.setObjectName("subtitleLabel")
        self.step_hint_label.setWordWrap(True)
        preview_layout.addWidget(self.step_hint_label)

        self.preview_input = QtWidgets.QPlainTextEdit()
        self.preview_input.setReadOnly(True)
        preview_layout.addWidget(self.preview_input, 1)
        content_splitter.addWidget(preview_card)
        content_splitter.setStretchFactor(0, 3)
        content_splitter.setStretchFactor(1, 2)
        root_layout.addWidget(content_splitter, 1)

        self.scroll_area.setWidget(self.content_widget)
        outer_layout.addWidget(self.scroll_area, 1)

        actions = QtWidgets.QHBoxLayout()
        self.validation_label = QtWidgets.QLabel()
        self.validation_label.setObjectName("subtitleLabel")
        self.validation_label.setWordWrap(True)
        self.cancel_button = make_button("Cancel", "secondary")
        self.save_button = make_button("Save Step")
        actions.addWidget(self.validation_label, 1)
        actions.addWidget(self.cancel_button)
        actions.addWidget(self.save_button)
        outer_layout.addLayout(actions)

        self.step_type_combo.currentIndexChanged.connect(self._on_step_type_changed)
        self.preset_combo.currentIndexChanged.connect(self._update_preset_description)
        self.apply_template_button.clicked.connect(self.apply_template)
        self.apply_preset_button.clicked.connect(self.apply_preset)
        self.cancel_button.clicked.connect(self.reject)
        self.save_button.clicked.connect(self._validate_and_accept)
        self.step_position_input.valueChanged.connect(self._refresh_preview)
        self.step_name_input.textChanged.connect(self._refresh_preview)
        self.step_enabled_check.stateChanged.connect(self._refresh_preview)

    def showEvent(self, event: QtGui.QShowEvent) -> None:
        super().showEvent(event)
        self._apply_screen_constraints()

    def _apply_screen_constraints(self) -> None:
        screen = self.screen() or QtWidgets.QApplication.primaryScreen()
        if screen is None:
            if not self._applied_screen_constraints:
                self.resize(980, 720)
            return

        available = screen.availableGeometry()
        max_width = max(720, available.width() - 40)
        max_height = max(520, available.height() - 40)
        preferred_width = min(980, max_width)
        preferred_height = min(760, max_height)

        self.setMaximumSize(max_width, max_height)
        self.resize(preferred_width, preferred_height)
        self.setSizeGripEnabled(True)
        self._applied_screen_constraints = True

    def _load_data(self, default_position: int) -> None:
        self._is_loading = True
        if self.step_data:
            parameters = json.loads(self.step_data.get("parameters") or "{}")
            self.step_position_input.setValue(int(self.step_data["position"]))
            self.step_name_input.setText(self.step_data["name"])
            self.step_enabled_check.setChecked(bool(self.step_data["is_enabled"]))
            index = self.step_type_combo.findData(self.step_data["step_type"])
            if index >= 0:
                self.step_type_combo.setCurrentIndex(index)
            self._build_dynamic_fields(parameters)
        else:
            self.step_position_input.setValue(default_position)
            self.step_enabled_check.setChecked(True)
            definition = definition_for(self.step_type_combo.currentData())
            self._build_dynamic_fields(definition.template)
            self.step_name_input.setText(definition.label)
            self._load_policy_into_widgets(default_execution_policy())
            self._load_flow_into_widgets(default_flow_control())
        if self.step_data:
            self._load_policy_into_widgets(parameters)
            self._load_flow_into_widgets(parameters)
        self._is_loading = False
        self._refresh_preview()

    def _on_step_type_changed(self) -> None:
        if self._is_loading:
            return
        self.extra_parameters = {}
        definition = definition_for(self.step_type_combo.currentData())
        self._build_dynamic_fields(definition.template)
        if not self.step_name_input.text().strip():
            self.step_name_input.setText(definition.label)
        self._refresh_preview()

    def _build_dynamic_fields(self, parameters: dict[str, object]) -> None:
        while self.form_fields_layout.rowCount():
            self.form_fields_layout.removeRow(0)

        definition = definition_for(self.step_type_combo.currentData())
        self.field_widgets.clear()
        self.field_definitions = {field.key: field for field in definition.fields}
        self.extra_parameters = {
            key: value for key, value in parameters.items() if key not in self.field_definitions
        }

        for field_definition in definition.fields:
            widget = self._create_widget_for_field(field_definition)
            self.field_widgets[field_definition.key] = widget
            self.form_fields_layout.addRow(make_form_label(field_definition.label), widget)
            self._connect_widget(widget)

        self._load_parameters_into_widgets(parameters)
        self._rebuild_preset_combo(definition.key)
        self._update_definition_hint()

    def _create_widget_for_field(self, field_definition: StepField) -> QtWidgets.QWidget:
        if field_definition.field_type == "bool":
            widget = QtWidgets.QCheckBox()
            widget.setChecked(bool(field_definition.default))
            return widget

        if field_definition.key == "target_workflow_id":
            widget = QtWidgets.QComboBox()
            for workflow_id, workflow_name in self.workflow_choices:
                widget.addItem(f"{workflow_name} (ID: {workflow_id})", workflow_id)
            return widget

        if field_definition.field_type == "combo":
            widget = QtWidgets.QComboBox()
            for value, label in field_definition.options:
                widget.addItem(label, value)
            return widget

        if field_definition.field_type == "int":
            widget = QtWidgets.QSpinBox()
            widget.setRange(
                int(field_definition.min_value if field_definition.min_value is not None else -999999),
                int(field_definition.max_value if field_definition.max_value is not None else 999999),
            )
            return widget

        if field_definition.field_type == "float":
            widget = QtWidgets.QDoubleSpinBox()
            widget.setDecimals(field_definition.decimals)
            widget.setSingleStep(0.1)
            widget.setRange(
                float(field_definition.min_value if field_definition.min_value is not None else -999999),
                float(field_definition.max_value if field_definition.max_value is not None else 999999),
            )
            return widget

        if field_definition.field_type == "textarea":
            widget = QtWidgets.QPlainTextEdit()
            widget.setFixedHeight(84)
            widget.setPlaceholderText(field_definition.placeholder)
            return widget

        widget = QtWidgets.QLineEdit()
        widget.setPlaceholderText(field_definition.placeholder)
        return widget

    def _connect_widget(self, widget: QtWidgets.QWidget) -> None:
        if isinstance(widget, QtWidgets.QLineEdit):
            widget.textChanged.connect(self._refresh_preview)
        elif isinstance(widget, QtWidgets.QPlainTextEdit):
            widget.textChanged.connect(self._refresh_preview)
        elif isinstance(widget, QtWidgets.QCheckBox):
            widget.stateChanged.connect(self._refresh_preview)
        elif isinstance(widget, QtWidgets.QComboBox):
            widget.currentIndexChanged.connect(self._refresh_preview)
        elif isinstance(widget, QtWidgets.QSpinBox):
            widget.valueChanged.connect(self._refresh_preview)
        elif isinstance(widget, QtWidgets.QDoubleSpinBox):
            widget.valueChanged.connect(self._refresh_preview)

    def _load_parameters_into_widgets(self, parameters: dict[str, object]) -> None:
        for key, widget in self.field_widgets.items():
            field_definition = self.field_definitions[key]
            value = parameters.get(key, field_definition.default)
            if isinstance(widget, QtWidgets.QLineEdit):
                widget.setText("" if value is None else str(value))
            elif isinstance(widget, QtWidgets.QPlainTextEdit):
                widget.setPlainText("" if value is None else str(value))
            elif isinstance(widget, QtWidgets.QCheckBox):
                widget.setChecked(bool(value))
            elif isinstance(widget, QtWidgets.QComboBox):
                index = widget.findData(value)
                if index >= 0:
                    widget.setCurrentIndex(index)
                elif widget.count() > 0:
                    widget.setCurrentIndex(0)
                elif field_definition.key == "target_workflow_id":
                    widget.addItem(f"Workflow ID: {value}", value)
                    widget.setCurrentIndex(0)
            elif isinstance(widget, QtWidgets.QSpinBox):
                widget.setValue(int(value or 0))
            elif isinstance(widget, QtWidgets.QDoubleSpinBox):
                widget.setValue(float(value or 0))

    def _load_policy_into_widgets(self, parameters: dict[str, object]) -> None:
        defaults = default_execution_policy()
        for key, widget in self.policy_widgets.items():
            value = parameters.get(key, defaults[key])
            if isinstance(widget, QtWidgets.QCheckBox):
                widget.setChecked(bool(value))
            elif isinstance(widget, QtWidgets.QComboBox):
                index = widget.findData(value)
                if index >= 0:
                    widget.setCurrentIndex(index)
            elif isinstance(widget, QtWidgets.QSpinBox):
                widget.setValue(int(value or 0))
            elif isinstance(widget, QtWidgets.QDoubleSpinBox):
                widget.setValue(float(value or 0))

    def _load_flow_into_widgets(self, parameters: dict[str, object]) -> None:
        defaults = default_flow_control()
        for key, widget in self.flow_widgets.items():
            value = parameters.get(key, defaults[key])
            if isinstance(widget, QtWidgets.QLineEdit):
                widget.setText("" if value is None else str(value))
            elif isinstance(widget, QtWidgets.QPlainTextEdit):
                widget.setPlainText("" if value is None else str(value))
            elif isinstance(widget, QtWidgets.QCheckBox):
                widget.setChecked(bool(value))
            elif isinstance(widget, QtWidgets.QComboBox):
                index = widget.findData(value)
                if index >= 0:
                    widget.setCurrentIndex(index)
            elif isinstance(widget, QtWidgets.QSpinBox):
                widget.setValue(int(value or 0))
            elif isinstance(widget, QtWidgets.QDoubleSpinBox):
                widget.setValue(float(value or 0))

    def _rebuild_preset_combo(self, step_type: str) -> None:
        presets = preset_map_for(step_type)
        self.preset_combo.blockSignals(True)
        self.preset_combo.clear()
        self.preset_combo.addItem("No Preset", "")
        for preset_label in presets:
            self.preset_combo.addItem(preset_label, preset_label)
        self.preset_combo.blockSignals(False)
        has_presets = bool(presets)
        self.preset_combo.setEnabled(has_presets)
        self.apply_preset_button.setEnabled(has_presets)
        self._update_preset_description()

    def _update_preset_description(self) -> None:
        preset_label = self.preset_combo.currentData()
        if not preset_label:
            self.preset_description_label.setText("ใช้ preset เพื่อเติมค่าที่ใช้บ่อยอย่างรวดเร็ว")
            return
        preset = preset_map_for(self.step_type_combo.currentData()).get(preset_label)
        self.preset_description_label.setText(preset.description if preset else "")

    def _update_definition_hint(self) -> None:
        definition = definition_for(self.step_type_combo.currentData())
        self.step_hint_label.setText(f"{definition.description}\nTemplate: {definition.template_json()}")

    def apply_template(self) -> None:
        definition = definition_for(self.step_type_combo.currentData())
        self.extra_parameters = {}
        self._load_parameters_into_widgets(definition.template)
        self._refresh_preview()

    def apply_preset(self) -> None:
        preset_label = self.preset_combo.currentData()
        if not preset_label:
            return
        preset = preset_map_for(self.step_type_combo.currentData()).get(preset_label)
        if not preset:
            return
        self.extra_parameters = {}
        self._load_parameters_into_widgets(preset.parameters)
        self._refresh_preview()

    def _collect_parameters(self) -> dict[str, object]:
        parameters: dict[str, object] = dict(self.extra_parameters)
        for key, widget in self.field_widgets.items():
            field_definition = self.field_definitions[key]
            value = self._widget_value(widget)
            if field_definition.field_type in {"text", "textarea"}:
                if str(value).strip():
                    parameters[key] = str(value).strip()
                continue
            if field_definition.field_type == "combo":
                if key == "direction":
                    parameters[key] = value
                elif str(value).strip():
                    parameters[key] = value
                continue
            parameters[key] = value
        for key, widget in self.policy_widgets.items():
            parameters[key] = self._widget_value(widget)
        for key, widget in self.flow_widgets.items():
            value = self._widget_value(widget)
            if isinstance(widget, (QtWidgets.QLineEdit, QtWidgets.QPlainTextEdit)):
                if str(value).strip():
                    parameters[key] = str(value).strip()
                continue
            parameters[key] = value
        return parameters

    def _widget_value(self, widget: QtWidgets.QWidget) -> object:
        if isinstance(widget, QtWidgets.QLineEdit):
            return widget.text()
        if isinstance(widget, QtWidgets.QPlainTextEdit):
            return widget.toPlainText()
        if isinstance(widget, QtWidgets.QCheckBox):
            return widget.isChecked()
        if isinstance(widget, QtWidgets.QComboBox):
            return widget.currentData()
        if isinstance(widget, QtWidgets.QSpinBox):
            return widget.value()
        if isinstance(widget, QtWidgets.QDoubleSpinBox):
            value = widget.value()
            return int(value) if float(value).is_integer() else value
        return None

    def _refresh_preview(self) -> None:
        definition = definition_for(self.step_type_combo.currentData())
        parameters = self._collect_parameters()
        self.preview_input.setPlainText(json.dumps(parameters, indent=2, ensure_ascii=False))
        self._update_definition_hint()
        errors = validate_step_parameters(definition.key, parameters)
        self.validation_label.setText("พร้อมบันทึก" if not errors else "Validation: " + " | ".join(errors))

    def _validate_and_accept(self) -> None:
        if not self.step_name_input.text().strip():
            QtWidgets.QMessageBox.warning(self, "Missing name", "กรุณากรอกชื่อ step")
            return
        parameters = self._collect_parameters()
        errors = validate_step_parameters(self.step_type_combo.currentData(), parameters)
        if errors:
            QtWidgets.QMessageBox.warning(self, "Invalid step", "\n".join(errors))
            return
        self.accept()

    def payload(self) -> dict[str, object]:
        return {
            "position": self.step_position_input.value(),
            "name": self.step_name_input.text().strip(),
            "step_type": self.step_type_combo.currentData(),
            "parameters": self._collect_parameters(),
            "is_enabled": self.step_enabled_check.isChecked(),
        }
