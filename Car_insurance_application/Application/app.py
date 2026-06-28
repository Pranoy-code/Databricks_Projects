import streamlit as st
import base64
from PIL import Image
from databricks.sdk import WorkspaceClient

# ==================================================
# CONFIGURATION
# ==================================================

workspace = WorkspaceClient()
SERVING_ENDPOINT_NAME = "serving-image-classification"

# ==================================================
# PAGE CONFIG
# ==================================================

st.set_page_config(
    page_title="Smart Claims AI",
    page_icon="🚗",
    layout="centered"
)

st.title("🚗 Smart Claims AI")
st.markdown("Automated Vehicle Claim Assessment")

# ==================================================
# MODEL SERVING
# ==================================================

def invoke_image_model(image_bytes):
    encoded_image = base64.b64encode(image_bytes).decode("utf-8")
    response = workspace.serving_endpoints.query(
        name=SERVING_ENDPOINT_NAME,
        dataframe_records=[{"content": encoded_image}]
    )
    return response


def parse_prediction(response):
    try:
        predictions = response.predictions
        prediction = predictions[0]
        damage_class = prediction["label"]
        confidence = prediction["score"]
        return damage_class, confidence
    except Exception:
        return "Unknown", 0.0


# ==================================================
# RISK CALCULATION
# ==================================================

def calculate_risk(claim_data, damage_class, confidence):
    risk_score = 0
    reasons = []
    
    # AI Damage Assessment
    if damage_class == "Major Damage":
        risk_score += 3
        reasons.append("🔴 Major Damage Detected by AI")
    elif damage_class == "Total Loss":
        risk_score += 5
        reasons.append("🔴 Total Loss Detected by AI")
    elif damage_class == "Minor Damage":
        risk_score += 0
        reasons.append("🟢 Minor Damage Detected by AI")
    
    if confidence < 0.6:
        risk_score += 1
        reasons.append("⚠️ Low AI Confidence")
    
    # Self-Assessed Severity
    self_severity = claim_data.get("self_severity", "")
    if self_severity == "Total Loss":
        risk_score += 2
        reasons.append("🔴 Customer Reports Total Loss")
    elif self_severity == "Major":
        risk_score += 1
        reasons.append("⚠️ Customer Reports Major Damage")
    
    # Compare AI vs Self-Assessment
    ai_high = damage_class in ["Major Damage", "Total Loss"]
    self_high = self_severity in ["Major", "Total Loss"]
    if ai_high != self_high:
        risk_score += 1
        reasons.append("⚠️ Mismatch: AI vs Customer Assessment")
    
    # Claim Amount
    claim_amount = claim_data.get("claim_amount", 0)
    if claim_amount > 50000:
        risk_score += 2
        reasons.append(f"⚠️ High Claim Amount: ${claim_amount:,.2f}")
    elif claim_amount > 30000:
        risk_score += 1
        reasons.append(f"⚠️ Elevated Claim Amount: ${claim_amount:,.2f}")
    
    return risk_score, reasons


def get_decision(score):
    if score >= 6:
        return "REJECT", "❌", "High risk detected. Claim requires rejection."
    elif score >= 3:
        return "MANUAL REVIEW", "⚠️", "Moderate risk. Human review required."
    else:
        return "APPROVE", "✅", "Low risk. Claim can be auto-approved."


# ==================================================
# UI
# ==================================================

st.markdown("---")

# Image Upload
st.subheader("📸 Upload Vehicle Damage Image")
uploaded_file = st.file_uploader(
    "Upload clear photo of vehicle damage",
    type=["jpg", "jpeg", "png"],
    help="Upload a JPG or PNG image showing the vehicle damage"
)

if uploaded_file:
    image = Image.open(uploaded_file)
    st.image(image, caption="Uploaded Vehicle Damage Image", use_container_width=True)

st.markdown("---")

# Claim Form
st.subheader("📋 Claim Information")

with st.form("claim_form"):
    
    policy_number = st.text_input(
        "Policy Number *",
        placeholder="e.g., 2024001",
        help="Enter the insurance policy number"
    )
    
    accident_location = st.text_input(
        "Accident Location *",
        placeholder="e.g., Highway 101, San Francisco, CA",
        help="Where did the accident occur?"
    )
    
    claim_amount = st.number_input(
        "Claim Amount (Rs) *",
        min_value=0.0,
        max_value=500000.0,
        value=10000.0,
        step=500.0,
        help="Total claim amount requested"
    )
    
    self_severity = st.selectbox(
        "Self-Assessed Severity *",
        options=["Select...", "Minor", "Moderate", "Major", "Total Loss"],
        help="How would you assess the damage severity?"
    )
    
    st.markdown("---")
    submit_button = st.form_submit_button(
        "🔍 Analyze Claim",
        use_container_width=True,
        type="primary"
    )

# ==================================================
# PROCESS SUBMISSION
# ==================================================

if submit_button:
    # Validation
    errors = []
    if not policy_number:
        errors.append("Policy Number is required")
    if not accident_location:
        errors.append("Accident Location is required")
    if claim_amount <= 0:
        errors.append("Claim Amount must be greater than 0")
    if self_severity == "Select...":
        errors.append("Self-Assessed Severity is required")
    if uploaded_file is None:
        errors.append("Vehicle damage image is required")
    
    if errors:
        for error in errors:
            st.error(f"❌ {error}")
        st.stop()
    
    # Collect claim data
    claim_data = {
        "policy_number": policy_number,
        "accident_location": accident_location,
        "claim_amount": claim_amount,
        "self_severity": self_severity
    }
    
    # Run AI Analysis
    with st.spinner("🤖 Running AI damage assessment..."):
        try:
            image_bytes = uploaded_file.getvalue()
            model_response = invoke_image_model(image_bytes)
            damage_class, confidence = parse_prediction(model_response)
        except Exception as e:
            st.error(f"❌ Model inference failed: {str(e)}")
            st.info("💡 Ensure 'serving-image-classification' endpoint is running and accessible.")
            st.stop()
    
    # Calculate Risk
    score, reasons = calculate_risk(claim_data, damage_class, confidence)
    
    # Get Decision
    decision, icon, message = get_decision(score)
    
    # ==================================================
    # DISPLAY RESULTS
    # ==================================================
    
    st.markdown("---")
    st.success("✅ Claim Analysis Complete!")
    
    # Claim Summary
    st.subheader("📄 Claim Summary")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.metric("Policy Number", policy_number)
        st.metric("Claim Amount", f"Rs {claim_amount:,.2f}")
    
    with col2:
        st.metric("Accident Location", accident_location)
        st.metric("Self-Assessed", self_severity)
    
    # AI Assessment
    st.subheader("🤖 AI Damage Assessment")
    
    col_ai1, col_ai2 = st.columns(2)
    
    with col_ai1:
        st.metric("AI Classification", damage_class)
    
    with col_ai2:
        st.metric("AI Confidence", f"{confidence:.1%}")
    
    # Risk Assessment
    st.subheader("⚠️ Risk Assessment")
    
    col_risk1, col_risk2 = st.columns([1, 2])
    
    with col_risk1:
        st.metric("Risk Score", score)
    
    with col_risk2:
        st.markdown("**Risk Factors:**")
        if reasons:
            for reason in reasons:
                st.write(f"• {reason}")
        else:
            st.write("✅ No significant risk indicators")
    
    # Final Decision
    st.markdown("---")
    st.subheader("🎯 Final Decision")
    
    if decision == "APPROVE":
        st.success(f"{icon} **{decision}** (Risk Score: {score})")
        st.info(message)
    elif decision == "MANUAL REVIEW":
        st.warning(f"{icon} **{decision}** (Risk Score: {score})")
        st.info(message)
    else:
        st.error(f"{icon} **{decision}** (Risk Score: {score})")
        st.info(message)
