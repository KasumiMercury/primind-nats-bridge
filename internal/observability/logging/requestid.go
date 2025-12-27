package logging

import "github.com/google/uuid"

func ValidateAndExtractRequestID(incoming string) string {
	if incoming == "" {
		return generateRequestID()
	}

	parsed, err := uuid.Parse(incoming)
	if err != nil {
		return generateRequestID()
	}

	if parsed.Version() != 7 {
		return generateRequestID()
	}

	return incoming
}

func generateRequestID() string {
	id, err := uuid.NewV7()
	if err != nil {
		panic(err)
	}

	return id.String()
}
