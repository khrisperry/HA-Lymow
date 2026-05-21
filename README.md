# HA-Lymow

Home Assistant custom integration for the Lymow mower.

## Integration installation

1. Copy `custom_components/lymow` into your Home Assistant `config/custom_components` folder.
2. Restart Home Assistant.
3. Open Settings -> Devices & Services -> Add Integration and choose `Lymow`.
4. Enter the AWS Cognito and AWS IoT credentials from your Lymow test environment.

## Configuration values

- **Lymow Thing Name**
- **Lymow IoT Endpoint**
- **Lymow Email**
- **Lymow Password**
- **Cognito User Pool ID**
- **Cognito Client ID**
- **Cognito Identity Pool ID**
- **AWS Region**
- **Cognito Region**

## Git workflow

To save your current changes and upload them to GitHub:

```bash
cd "c:\Users\kperry\Documents\GitHub\HA-Lymow"
git add .
git commit -m "Add Lymow Home Assistant integration"
git push origin main
```

If you need to set a new remote, use:

```bash
git remote add origin <your-repo-url>
git push -u origin main
```

## Notes

- This integration now connects directly to AWS IoT and populates Home Assistant sensors.
- The `scripts/lymow_ha_bridge.py` file is retained for legacy testing but is not required for the integration.
- Keep `.env` local and do not commit secrets. Use `.env.example` for placeholder values.
