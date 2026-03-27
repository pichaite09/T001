from __future__ import annotations

import json

from PySide6 import QtCore, QtWidgets

from automation_studio.models import (
    STEP_DEFINITIONS,
    StepField,
    WATCHER_ACTION_FIELDS,
    WATCHER_CONDITION_FIELDS,
    WATCHER_SCOPE_OPTIONS,
    WATCHER_STAGE_OPTIONS,
    default_watcher_policy,
    definition_for,
    validate_step_parameters,
    validate_watcher_config,
    watcher_presets,
)
from automation_studio.ui.widgets import CardFrame, make_button, make_form_label


class WatcherEditorDialog(QtWidgets.QDialog):
    def __init__(
        self,
        workflows: list[dict],
        devices: list[dict],
        watcher_service,
        parent: QtWidgets.QWidget | None = None,
        watcher_data: dict | None = None,
    ) -> None:
        super().__init__(parent)
        self.workflows = workflows
        self.devices = devices
        self.watcher_service = watcher_service
        self.watcher_data = watcher_data
        self._is_loading = False
        self.condition_widgets: dict[str, QtWidgets.QWidget] = {}
        self.action_widgets: dict[str, QtWidgets.QWidget] = {}
        self.step_action_widgets: dict[str, QtWidgets.QWidget] = {}
        self.policy_widgets: dict[str, QtWidgets.QWidget] = {}
        self.stage_checks: dict[str, QtWidgets.QCheckBox] = {}
        self.setWindowTitle("Watcher Editor")
        self.setModal(True)
        self.resize(980, 760)
        self._build_ui()
        self._load_data()

    def _build_ui(self) -> None:
        root = QtWidgets.QVBoxLayout(self)
        root.setContentsMargins(18, 18, 18, 18)
        root.setSpacing(12)

        title = QtWidgets.QLabel("Watcher Editor")
        title.setObjectName("titleLabel")
        subtitle = QtWidgets.QLabel("Build runtime guards with presets, typed forms, and device-side testing tools.")
        subtitle.setObjectName("subtitleLabel")
        root.addWidget(title)
        root.addWidget(subtitle)

        scroll = QtWidgets.QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QtWidgets.QFrame.Shape.NoFrame)
        root.addWidget(scroll, 1)

        content = QtWidgets.QWidget()
        scroll.setWidget(content)
        content_layout = QtWidgets.QVBoxLayout(content)
        content_layout.setContentsMargins(0, 0, 0, 0)
        content_layout.setSpacing(12)

        general_card = CardFrame()
        general_layout = QtWidgets.QGridLayout(general_card)
        general_layout.setContentsMargins(18, 18, 18, 18)
        general_layout.setHorizontalSpacing(12)
        general_layout.setVerticalSpacing(10)

        general_layout.addWidget(make_form_label("Preset"), 0, 0)
        self.preset_combo = QtWidgets.QComboBox()
        self.preset_combo.addItem("Custom", None)
        for preset in watcher_presets():
            self.preset_combo.addItem(preset.label, preset.label)
        general_layout.addWidget(self.preset_combo, 0, 1, 1, 2)
        self.apply_preset_button = make_button("Apply Preset", "secondary")
        general_layout.addWidget(self.apply_preset_button, 0, 3)

        self.preset_description_label = QtWidgets.QLabel()
        self.preset_description_label.setObjectName("subtitleLabel")
        self.preset_description_label.setWordWrap(True)
        general_layout.addWidget(self.preset_description_label, 1, 0, 1, 4)

        general_layout.addWidget(make_form_label("Name"), 2, 0)
        self.name_input = QtWidgets.QLineEdit()
        general_layout.addWidget(self.name_input, 2, 1)

        general_layout.addWidget(make_form_label("Priority"), 2, 2)
        self.priority_input = QtWidgets.QSpinBox()
        self.priority_input.setRange(0, 9999)
        self.priority_input.setValue(100)
        general_layout.addWidget(self.priority_input, 2, 3)

        general_layout.addWidget(make_form_label("Scope Type"), 3, 0)
        self.scope_type_combo = QtWidgets.QComboBox()
        for key, label in WATCHER_SCOPE_OPTIONS:
            self.scope_type_combo.addItem(label, key)
        general_layout.addWidget(self.scope_type_combo, 3, 1)

        general_layout.addWidget(make_form_label("Scope Target"), 3, 2)
        self.scope_target_combo = QtWidgets.QComboBox()
        general_layout.addWidget(self.scope_target_combo, 3, 3)

        self.enabled_check = QtWidgets.QCheckBox("Enabled")
        self.enabled_check.setChecked(True)
        general_layout.addWidget(self.enabled_check, 4, 0, 1, 4)
        content_layout.addWidget(general_card)

        top_split = QtWidgets.QSplitter()
        top_split.setChildrenCollapsible(False)
        content_layout.addWidget(top_split, 1)

        condition_card = CardFrame()
        condition_layout = QtWidgets.QVBoxLayout(condition_card)
        condition_layout.setContentsMargins(18, 18, 18, 18)
        condition_layout.setSpacing(10)
        condition_header = QtWidgets.QHBoxLayout()
        condition_header.addWidget(make_form_label("Condition Type"))
        self.condition_type_combo = QtWidgets.QComboBox()
        for condition_type in WATCHER_CONDITION_FIELDS:
            self.condition_type_combo.addItem(condition_type, condition_type)
        condition_header.addWidget(self.condition_type_combo, 1)
        condition_layout.addLayout(condition_header)

        self.condition_form_container = QtWidgets.QWidget()
        self.condition_form_layout = QtWidgets.QFormLayout(self.condition_form_container)
        self.condition_form_layout.setFieldGrowthPolicy(QtWidgets.QFormLayout.FieldGrowthPolicy.ExpandingFieldsGrow)
        self.condition_form_layout.setHorizontalSpacing(16)
        self.condition_form_layout.setVerticalSpacing(10)
        condition_layout.addWidget(self.condition_form_container)
        top_split.addWidget(condition_card)

        action_card = CardFrame()
        action_layout = QtWidgets.QVBoxLayout(action_card)
        action_layout.setContentsMargins(18, 18, 18, 18)
        action_layout.setSpacing(10)
        action_header = QtWidgets.QHBoxLayout()
        action_header.addWidget(make_form_label("Action Type"))
        self.action_type_combo = QtWidgets.QComboBox()
        for action_type in WATCHER_ACTION_FIELDS:
            self.action_type_combo.addItem(action_type, action_type)
        action_header.addWidget(self.action_type_combo, 1)
        action_layout.addLayout(action_header)

        self.action_form_container = QtWidgets.QWidget()
        self.action_form_layout = QtWidgets.QFormLayout(self.action_form_container)
        self.action_form_layout.setFieldGrowthPolicy(QtWidgets.QFormLayout.FieldGrowthPolicy.ExpandingFieldsGrow)
        self.action_form_layout.setHorizontalSpacing(16)
        self.action_form_layout.setVerticalSpacing(10)
        action_layout.addWidget(self.action_form_container)

        self.run_step_group = QtWidgets.QGroupBox("Run Step Action")
        run_step_layout = QtWidgets.QVBoxLayout(self.run_step_group)
        run_step_layout.setContentsMargins(12, 12, 12, 12)
        run_step_layout.setSpacing(10)

        run_step_type_row = QtWidgets.QHBoxLayout()
        run_step_type_row.addWidget(make_form_label("Step Type"))
        self.action_step_type_combo = QtWidgets.QComboBox()
        for definition in STEP_DEFINITIONS:
            self.action_step_type_combo.addItem(f"{definition.label} ({definition.key})", definition.key)
        run_step_type_row.addWidget(self.action_step_type_combo, 1)
        run_step_layout.addLayout(run_step_type_row)

        self.run_step_hint_label = QtWidgets.QLabel()
        self.run_step_hint_label.setObjectName("subtitleLabel")
        self.run_step_hint_label.setWordWrap(True)
        run_step_layout.addWidget(self.run_step_hint_label)

        self.run_step_form_container = QtWidgets.QWidget()
        self.run_step_form_layout = QtWidgets.QFormLayout(self.run_step_form_container)
        self.run_step_form_layout.setFieldGrowthPolicy(QtWidgets.QFormLayout.FieldGrowthPolicy.ExpandingFieldsGrow)
        self.run_step_form_layout.setHorizontalSpacing(16)
        self.run_step_form_layout.setVerticalSpacing(10)
        run_step_layout.addWidget(self.run_step_form_container)
        action_layout.addWidget(self.run_step_group)
        top_split.addWidget(action_card)
        top_split.setStretchFactor(0, 1)
        top_split.setStretchFactor(1, 1)

        lower_split = QtWidgets.QSplitter()
        lower_split.setChildrenCollapsible(False)
        content_layout.addWidget(lower_split, 1)

        policy_card = CardFrame()
        policy_layout = QtWidgets.QVBoxLayout(policy_card)
        policy_layout.setContentsMargins(18, 18, 18, 18)
        policy_layout.setSpacing(10)

        policy_title = QtWidgets.QLabel("Policy")
        policy_title.setObjectName("subtitleLabel")
        policy_layout.addWidget(policy_title)

        policy_form = QtWidgets.QFormLayout()
        policy_form.setFieldGrowthPolicy(QtWidgets.QFormLayout.FieldGrowthPolicy.ExpandingFieldsGrow)
        policy_form.setHorizontalSpacing(16)
        policy_form.setVerticalSpacing(10)

        self.policy_widgets["cooldown_seconds"] = self._create_widget_for_field(
            StepField("cooldown_seconds", "Cooldown Seconds", field_type="float", default=3.0, min_value=0, decimals=2)
        )
        self.policy_widgets["debounce_count"] = self._create_widget_for_field(
            StepField("debounce_count", "Debounce Count", field_type="int", default=1, min_value=1)
        )
        self.policy_widgets["max_triggers_per_run"] = self._create_widget_for_field(
            StepField("max_triggers_per_run", "Max Triggers Per Run", field_type="int", default=0, min_value=0)
        )
        self.policy_widgets["stop_after_match"] = self._create_widget_for_field(
            StepField("stop_after_match", "Stop After Match", field_type="bool", default=False)
        )
        self.policy_widgets["match_mode"] = self._create_widget_for_field(
            StepField(
                "match_mode",
                "Match Mode",
                field_type="combo",
                default="first_match",
                options=(("first_match", "First Match"), ("continue", "Continue Matching")),
            )
        )
        policy_form.addRow(make_form_label("Cooldown Seconds"), self.policy_widgets["cooldown_seconds"])
        policy_form.addRow(make_form_label("Debounce Count"), self.policy_widgets["debounce_count"])
        policy_form.addRow(make_form_label("Max Triggers Per Run"), self.policy_widgets["max_triggers_per_run"])
        policy_form.addRow(make_form_label("Match Mode"), self.policy_widgets["match_mode"])
        policy_form.addRow(make_form_label("Stop After Match"), self.policy_widgets["stop_after_match"])
        policy_layout.addLayout(policy_form)

        stages_label = make_form_label("Active Stages")
        policy_layout.addWidget(stages_label)
        stages_row = QtWidgets.QHBoxLayout()
        for stage_key, stage_label in WATCHER_STAGE_OPTIONS:
            checkbox = QtWidgets.QCheckBox(stage_label)
            self.stage_checks[stage_key] = checkbox
            stages_row.addWidget(checkbox)
            checkbox.toggled.connect(self._refresh_preview)
        stages_row.addStretch(1)
        policy_layout.addLayout(stages_row)
        lower_split.addWidget(policy_card)

        tools_card = CardFrame()
        tools_layout = QtWidgets.QVBoxLayout(tools_card)
        tools_layout.setContentsMargins(18, 18, 18, 18)
        tools_layout.setSpacing(10)

        tools_title = QtWidgets.QLabel("Test Tools")
        tools_title.setObjectName("subtitleLabel")
        tools_layout.addWidget(tools_title)

        test_device_row = QtWidgets.QHBoxLayout()
        test_device_row.addWidget(make_form_label("Device"))
        self.test_device_combo = QtWidgets.QComboBox()
        for device in self.devices:
            self.test_device_combo.addItem(f"{device['name']} ({device['serial']})", device["id"])
        test_device_row.addWidget(self.test_device_combo, 1)
        tools_layout.addLayout(test_device_row)

        test_buttons = QtWidgets.QHBoxLayout()
        self.test_condition_button = make_button("Test Condition", "secondary")
        self.test_action_button = make_button("Test Action", "secondary")
        test_buttons.addWidget(self.test_condition_button)
        test_buttons.addWidget(self.test_action_button)
        test_buttons.addStretch(1)
        tools_layout.addLayout(test_buttons)

        self.test_result_label = QtWidgets.QLabel("Select a device to test the current condition or action.")
        self.test_result_label.setObjectName("subtitleLabel")
        self.test_result_label.setWordWrap(True)
        tools_layout.addWidget(self.test_result_label)

        preview_title = QtWidgets.QLabel("Watcher Preview")
        preview_title.setObjectName("subtitleLabel")
        tools_layout.addWidget(preview_title)
        self.preview_input = QtWidgets.QPlainTextEdit()
        self.preview_input.setReadOnly(True)
        tools_layout.addWidget(self.preview_input, 1)
        lower_split.addWidget(tools_card)
        lower_split.setStretchFactor(0, 1)
        lower_split.setStretchFactor(1, 1)

        actions = QtWidgets.QHBoxLayout()
        self.validation_label = QtWidgets.QLabel()
        self.validation_label.setObjectName("subtitleLabel")
        self.validation_label.setWordWrap(True)
        self.cancel_button = make_button("Cancel", "secondary")
        self.save_button = make_button("Save Watcher")
        actions.addWidget(self.validation_label, 1)
        actions.addWidget(self.cancel_button)
        actions.addWidget(self.save_button)
        root.addLayout(actions)

        self.scope_type_combo.currentIndexChanged.connect(self._refresh_scope_targets)
        self.preset_combo.currentIndexChanged.connect(self._update_preset_description)
        self.apply_preset_button.clicked.connect(self._apply_selected_preset)
        self.condition_type_combo.currentIndexChanged.connect(self._on_condition_type_changed)
        self.action_type_combo.currentIndexChanged.connect(self._on_action_type_changed)
        self.action_step_type_combo.currentIndexChanged.connect(self._on_action_step_type_changed)
        self.test_condition_button.clicked.connect(self._test_condition)
        self.test_action_button.clicked.connect(self._test_action)
        self.cancel_button.clicked.connect(self.reject)
        self.save_button.clicked.connect(self._validate_and_accept)

        for widget in (
            self.name_input,
            self.priority_input,
            self.scope_type_combo,
            self.scope_target_combo,
            self.enabled_check,
            self.condition_type_combo,
            self.action_type_combo,
            self.action_step_type_combo,
        ):
            self._connect_widget(widget)

    def _load_data(self) -> None:
        self._is_loading = True
        self._refresh_scope_targets()
        self._build_condition_form(self.condition_type_combo.currentData(), {})
        self._build_action_form(self.action_type_combo.currentData(), {})
        self._load_policy(default_watcher_policy())

        if self.watcher_data:
            condition = json.loads(self.watcher_data.get("condition_json") or "{}")
            action = json.loads(self.watcher_data.get("action_json") or "{}")
            policy = default_watcher_policy()
            policy.update(json.loads(self.watcher_data.get("policy_json") or "{}"))

            self.name_input.setText(str(self.watcher_data.get("name") or ""))
            self.priority_input.setValue(int(self.watcher_data.get("priority", 100) or 100))
            self.enabled_check.setChecked(bool(self.watcher_data.get("is_enabled", 1)))
            scope_index = self.scope_type_combo.findData(self.watcher_data.get("scope_type"))
            if scope_index >= 0:
                self.scope_type_combo.setCurrentIndex(scope_index)
            self._refresh_scope_targets()
            target_index = self.scope_target_combo.findData(self.watcher_data.get("scope_id"))
            if target_index >= 0:
                self.scope_target_combo.setCurrentIndex(target_index)
            condition_index = self.condition_type_combo.findData(self.watcher_data.get("condition_type"))
            if condition_index >= 0:
                self.condition_type_combo.setCurrentIndex(condition_index)
            self._build_condition_form(self.condition_type_combo.currentData(), condition)
            action_index = self.action_type_combo.findData(self.watcher_data.get("action_type"))
            if action_index >= 0:
                self.action_type_combo.setCurrentIndex(action_index)
            self._build_action_form(self.action_type_combo.currentData(), action)
            self._load_policy(policy)
        else:
            self._update_preset_description()

        self._is_loading = False
        self._refresh_preview()

    def _create_widget_for_field(self, field_definition: StepField) -> QtWidgets.QWidget:
        if field_definition.field_type == "bool":
            widget = QtWidgets.QCheckBox()
            widget.setChecked(bool(field_definition.default))
            return widget
        if field_definition.field_type == "combo":
            widget = QtWidgets.QComboBox()
            for value, label in field_definition.options:
                widget.addItem(label, value)
            if field_definition.default not in ("", None):
                index = widget.findData(field_definition.default)
                if index >= 0:
                    widget.setCurrentIndex(index)
            return widget
        if field_definition.field_type == "int":
            widget = QtWidgets.QSpinBox()
            widget.setRange(
                int(field_definition.min_value if field_definition.min_value is not None else -999999),
                int(field_definition.max_value if field_definition.max_value is not None else 999999),
            )
            widget.setValue(int(field_definition.default or 0))
            return widget
        if field_definition.field_type == "float":
            widget = QtWidgets.QDoubleSpinBox()
            widget.setDecimals(field_definition.decimals)
            widget.setSingleStep(0.1)
            widget.setRange(
                float(field_definition.min_value if field_definition.min_value is not None else -999999),
                float(field_definition.max_value if field_definition.max_value is not None else 999999),
            )
            widget.setValue(float(field_definition.default or 0))
            return widget
        if field_definition.field_type == "textarea":
            widget = QtWidgets.QPlainTextEdit()
            widget.setFixedHeight(84)
            widget.setPlaceholderText(field_definition.placeholder)
            widget.setPlainText(str(field_definition.default or ""))
            return widget
        widget = QtWidgets.QLineEdit()
        widget.setPlaceholderText(field_definition.placeholder)
        widget.setText(str(field_definition.default or ""))
        return widget

    def _connect_widget(self, widget: QtWidgets.QWidget) -> None:
        if isinstance(widget, QtWidgets.QLineEdit):
            widget.textChanged.connect(self._refresh_preview)
        elif isinstance(widget, QtWidgets.QPlainTextEdit):
            widget.textChanged.connect(self._refresh_preview)
        elif isinstance(widget, QtWidgets.QCheckBox):
            widget.toggled.connect(self._refresh_preview)
        elif isinstance(widget, QtWidgets.QComboBox):
            widget.currentIndexChanged.connect(self._refresh_preview)
        elif isinstance(widget, (QtWidgets.QSpinBox, QtWidgets.QDoubleSpinBox)):
            widget.valueChanged.connect(self._refresh_preview)

    def _clear_form_layout(self, layout: QtWidgets.QFormLayout) -> None:
        while layout.rowCount() > 0:
            layout.removeRow(0)

    def _build_condition_form(self, condition_type: str, payload: dict) -> None:
        self._clear_form_layout(self.condition_form_layout)
        self.condition_widgets.clear()
        for field_definition in WATCHER_CONDITION_FIELDS.get(str(condition_type), ()):
            widget = self._create_widget_for_field(field_definition)
            self._set_widget_value(widget, payload.get(field_definition.key, field_definition.default))
            self._connect_widget(widget)
            self.condition_widgets[field_definition.key] = widget
            self.condition_form_layout.addRow(make_form_label(field_definition.label), widget)

    def _build_action_form(self, action_type: str, payload: dict) -> None:
        self._clear_form_layout(self.action_form_layout)
        self.action_widgets.clear()
        self.run_step_group.setVisible(str(action_type) == "run_step")
        if str(action_type) == "run_step":
            action_step_type = str(payload.get("step_type") or "click")
            index = self.action_step_type_combo.findData(action_step_type)
            if index >= 0:
                self.action_step_type_combo.setCurrentIndex(index)
            self._build_run_step_form(action_step_type, payload.get("parameters", {}))
            return
        for field_definition in WATCHER_ACTION_FIELDS.get(str(action_type), ()):
            widget = self._create_widget_for_field(field_definition)
            value = payload.get(field_definition.key, field_definition.default)
            if field_definition.key == "actions" and not isinstance(value, str):
                value = json.dumps(value, indent=2, ensure_ascii=False)
            self._set_widget_value(widget, value)
            self._connect_widget(widget)
            self.action_widgets[field_definition.key] = widget
            self.action_form_layout.addRow(make_form_label(field_definition.label), widget)

    def _build_run_step_form(self, step_type: str, payload: dict) -> None:
        self._clear_form_layout(self.run_step_form_layout)
        self.step_action_widgets.clear()
        definition = definition_for(step_type)
        self.run_step_hint_label.setText(definition.description)
        for field_definition in definition.fields:
            widget = self._create_widget_for_field(field_definition)
            self._set_widget_value(widget, payload.get(field_definition.key, field_definition.default))
            self._connect_widget(widget)
            self.step_action_widgets[field_definition.key] = widget
            self.run_step_form_layout.addRow(make_form_label(field_definition.label), widget)

    def _load_policy(self, policy: dict) -> None:
        self._set_widget_value(self.policy_widgets["cooldown_seconds"], policy.get("cooldown_seconds", 3.0))
        self._set_widget_value(self.policy_widgets["debounce_count"], policy.get("debounce_count", 1))
        self._set_widget_value(self.policy_widgets["max_triggers_per_run"], policy.get("max_triggers_per_run", 0))
        self._set_widget_value(self.policy_widgets["match_mode"], policy.get("match_mode", "first_match"))
        self._set_widget_value(self.policy_widgets["stop_after_match"], bool(policy.get("stop_after_match", False)))
        active_stages = {str(item) for item in policy.get("active_stages", [])}
        for stage_key, checkbox in self.stage_checks.items():
            checkbox.setChecked(stage_key in active_stages)

    def _refresh_scope_targets(self) -> None:
        current_scope = self.scope_type_combo.currentData()
        current_value = self.scope_target_combo.currentData()
        self.scope_target_combo.blockSignals(True)
        self.scope_target_combo.clear()
        if current_scope == "global":
            self.scope_target_combo.addItem("Global", None)
            self.scope_target_combo.setEnabled(False)
        else:
            self.scope_target_combo.setEnabled(True)
            items = self.workflows if current_scope == "workflow" else self.devices
            for item in items:
                self.scope_target_combo.addItem(item["name"], item["id"])
            if current_value is not None:
                index = self.scope_target_combo.findData(current_value)
                if index >= 0:
                    self.scope_target_combo.setCurrentIndex(index)
        self.scope_target_combo.blockSignals(False)
        if not self._is_loading:
            self._refresh_preview()

    def _update_preset_description(self) -> None:
        preset = self._selected_preset()
        self.preset_description_label.setText("" if not preset else preset.description)

    def _selected_preset(self):
        label = self.preset_combo.currentData()
        for preset in watcher_presets():
            if preset.label == label:
                return preset
        return None

    def _apply_selected_preset(self) -> None:
        preset = self._selected_preset()
        if not preset:
            return
        self._is_loading = True
        self.name_input.setText(preset.name)
        self.priority_input.setValue(100)
        self.enabled_check.setChecked(True)
        scope_index = self.scope_type_combo.findData("global")
        if scope_index >= 0:
            self.scope_type_combo.setCurrentIndex(scope_index)
        self._refresh_scope_targets()
        condition_index = self.condition_type_combo.findData(preset.condition_type)
        if condition_index >= 0:
            self.condition_type_combo.setCurrentIndex(condition_index)
        self._build_condition_form(preset.condition_type, preset.condition)
        action_index = self.action_type_combo.findData(preset.action_type)
        if action_index >= 0:
            self.action_type_combo.setCurrentIndex(action_index)
        self._build_action_form(preset.action_type, preset.action)
        self._load_policy(preset.policy)
        self._is_loading = False
        self._refresh_preview()

    def _on_condition_type_changed(self) -> None:
        if self._is_loading:
            return
        self._build_condition_form(str(self.condition_type_combo.currentData()), {})
        self._refresh_preview()

    def _on_action_type_changed(self) -> None:
        if self._is_loading:
            return
        self._build_action_form(str(self.action_type_combo.currentData()), {})
        self._refresh_preview()

    def _on_action_step_type_changed(self) -> None:
        if self._is_loading or str(self.action_type_combo.currentData()) != "run_step":
            return
        self._build_run_step_form(str(self.action_step_type_combo.currentData()), {})
        self._refresh_preview()

    def _collect_form_values(self, widgets: dict[str, QtWidgets.QWidget], field_map: tuple[StepField, ...]) -> dict[str, object]:
        payload: dict[str, object] = {}
        definitions = {field.key: field for field in field_map}
        for key, widget in widgets.items():
            field_definition = definitions[key]
            value = self._widget_value(widget)
            if field_definition.field_type in {"text", "textarea"}:
                if str(value).strip():
                    payload[key] = str(value).strip()
                continue
            if field_definition.field_type == "combo":
                if value not in ("", None):
                    payload[key] = value
                continue
            payload[key] = value
        return payload

    def _collect_condition(self) -> dict[str, object]:
        fields = WATCHER_CONDITION_FIELDS.get(str(self.condition_type_combo.currentData()), ())
        return self._collect_form_values(self.condition_widgets, fields)

    def _collect_action(self) -> dict[str, object]:
        action_type = str(self.action_type_combo.currentData())
        if action_type == "run_step":
            step_type = str(self.action_step_type_combo.currentData())
            step_definition = definition_for(step_type)
            parameters = self._collect_form_values(self.step_action_widgets, step_definition.fields)
            return {"step_type": step_type, "parameters": parameters}
        fields = WATCHER_ACTION_FIELDS.get(action_type, ())
        payload = self._collect_form_values(self.action_widgets, fields)
        if action_type == "action_chain" and "actions" in payload:
            try:
                payload["actions"] = json.loads(str(payload["actions"]))
            except json.JSONDecodeError:
                pass
        return payload

    def _collect_policy(self) -> dict[str, object]:
        return {
            "cooldown_seconds": self._widget_value(self.policy_widgets["cooldown_seconds"]),
            "debounce_count": self._widget_value(self.policy_widgets["debounce_count"]),
            "max_triggers_per_run": self._widget_value(self.policy_widgets["max_triggers_per_run"]),
            "match_mode": self._widget_value(self.policy_widgets["match_mode"]),
            "stop_after_match": self._widget_value(self.policy_widgets["stop_after_match"]),
            "active_stages": [stage_key for stage_key, checkbox in self.stage_checks.items() if checkbox.isChecked()],
        }

    def _refresh_preview(self) -> None:
        if self._is_loading:
            return
        condition = self._collect_condition()
        action = self._collect_action()
        policy = self._collect_policy()
        payload = {
            "name": self.name_input.text().strip(),
            "priority": int(self.priority_input.value()),
            "scope_type": str(self.scope_type_combo.currentData()),
            "scope_id": self.scope_target_combo.currentData(),
            "condition_type": str(self.condition_type_combo.currentData()),
            "condition": condition,
            "action_type": str(self.action_type_combo.currentData()),
            "action": action,
            "policy": policy,
            "is_enabled": self.enabled_check.isChecked(),
        }
        self.preview_input.setPlainText(json.dumps(payload, indent=2, ensure_ascii=False))

        errors = validate_watcher_config(
            name=self.name_input.text(),
            scope_type=str(self.scope_type_combo.currentData()),
            scope_id=self.scope_target_combo.currentData(),
            condition_type=str(self.condition_type_combo.currentData()),
            condition=condition,
            action_type=str(self.action_type_combo.currentData()),
            action=action,
            policy=policy,
        )
        if str(self.action_type_combo.currentData()) == "run_step":
            action_step_type = str(self.action_step_type_combo.currentData())
            errors.extend(validate_step_parameters(action_step_type, action.get("parameters", {})))
        self.validation_label.setText("Ready to save" if not errors else "Validation: " + " | ".join(errors))

    def _test_condition(self) -> None:
        device_id = self.test_device_combo.currentData()
        if device_id is None:
            self.test_result_label.setText("Select a device before testing.")
            return
        try:
            matched, message, metadata = self.watcher_service.test_condition(
                device_id=device_id,
                condition_type=str(self.condition_type_combo.currentData()),
                condition_text=json.dumps(self._collect_condition(), ensure_ascii=False),
            )
            status = "Matched" if matched else "Not Matched"
            self.test_result_label.setText(f"{status}: {message}\n{json.dumps(metadata, ensure_ascii=False)}")
        except Exception as exc:
            self.test_result_label.setText(f"Condition test failed: {exc}")

    def _test_action(self) -> None:
        device_id = self.test_device_combo.currentData()
        if device_id is None:
            self.test_result_label.setText("Select a device before testing.")
            return
        try:
            success, message, metadata = self.watcher_service.test_action(
                device_id=device_id,
                action_type=str(self.action_type_combo.currentData()),
                action_text=json.dumps(self._collect_action(), ensure_ascii=False),
            )
            status = "Success" if success else "Failed"
            self.test_result_label.setText(f"{status}: {message}\n{json.dumps(metadata, ensure_ascii=False)}")
        except Exception as exc:
            self.test_result_label.setText(f"Action test failed: {exc}")

    def _validate_and_accept(self) -> None:
        condition = self._collect_condition()
        action = self._collect_action()
        policy = self._collect_policy()
        errors = validate_watcher_config(
            name=self.name_input.text(),
            scope_type=str(self.scope_type_combo.currentData()),
            scope_id=self.scope_target_combo.currentData(),
            condition_type=str(self.condition_type_combo.currentData()),
            condition=condition,
            action_type=str(self.action_type_combo.currentData()),
            action=action,
            policy=policy,
        )
        if str(self.action_type_combo.currentData()) == "run_step":
            action_step_type = str(self.action_step_type_combo.currentData())
            errors.extend(validate_step_parameters(action_step_type, action.get("parameters", {})))
        if errors:
            QtWidgets.QMessageBox.warning(self, "Invalid watcher", "\n".join(errors))
            return
        self.accept()

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

    def _set_widget_value(self, widget: QtWidgets.QWidget, value: object) -> None:
        if isinstance(widget, QtWidgets.QLineEdit):
            widget.setText("" if value is None else str(value))
            return
        if isinstance(widget, QtWidgets.QPlainTextEdit):
            widget.setPlainText("" if value is None else str(value))
            return
        if isinstance(widget, QtWidgets.QCheckBox):
            widget.setChecked(bool(value))
            return
        if isinstance(widget, QtWidgets.QComboBox):
            index = widget.findData(value)
            if index >= 0:
                widget.setCurrentIndex(index)
            return
        if isinstance(widget, QtWidgets.QSpinBox):
            widget.setValue(int(value or 0))
            return
        if isinstance(widget, QtWidgets.QDoubleSpinBox):
            widget.setValue(float(value or 0))

    def payload(self) -> dict:
        return {
            "id": self.watcher_data.get("id") if self.watcher_data else None,
            "name": self.name_input.text().strip(),
            "priority": int(self.priority_input.value()),
            "scope_type": str(self.scope_type_combo.currentData()),
            "scope_id": self.scope_target_combo.currentData(),
            "condition_type": str(self.condition_type_combo.currentData()),
            "condition_text": json.dumps(self._collect_condition(), indent=2, ensure_ascii=False),
            "action_type": str(self.action_type_combo.currentData()),
            "action_text": json.dumps(self._collect_action(), indent=2, ensure_ascii=False),
            "policy_text": json.dumps(self._collect_policy(), indent=2, ensure_ascii=False),
            "is_enabled": self.enabled_check.isChecked(),
        }
