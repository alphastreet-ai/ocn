resource "aws_iam_role" "ecs_task_execution" {
  name = "${var.env}-ecs-task-execution-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect    = "Allow"
        Principal = {
          Service = "ecs-tasks.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })
}


resource "aws_iam_role_policy_attachment" "ecs_execution_managed" {
  role       = aws_iam_role.ecs_task_execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}


resource "aws_iam_role_policy" "secrets_read" {
  name = "${var.env}-ecs-secrets-read"
  role = aws_iam_role.ecs_task_execution.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = ["secretsmanager:GetSecretValue"]
        Resource = [
          "arn:aws:secretsmanager:*:*:secret:ocn/${var.env}/*"
        ]
      }
    ]
  })
}


output "execution_role_arn" {
  value = aws_iam_role.ecs_task_execution.arn
}


resource "aws_iam_role" "ecs_task_exec_ssm" {
  name = "${var.env}-ecs-task-exec-ssm-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect    = "Allow"
        Principal = {
          Service = "ecs-tasks.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })
}


resource "aws_iam_role_policy" "ecs_task_exec_ssm" {
  name = "${var.env}-ecs-task-exec-ssm"
  role = aws_iam_role.ecs_task_exec_ssm.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "ssmmessages:CreateControlChannel",
          "ssmmessages:CreateDataChannel",
          "ssmmessages:OpenControlChannel",
          "ssmmessages:OpenDataChannel"
        ]
        Resource = "*"
      }
    ]
  })
}


output "task_exec_ssm_role_arn" {
  value = aws_iam_role.ecs_task_exec_ssm.arn
}



resource "aws_iam_openid_connect_provider" "github" {
  url             = "https://token.actions.githubusercontent.com"
  client_id_list  = ["sts.amazonaws.com"]
  thumbprint_list = ["6938fd4d98bab03faadb97b34396831e3780aea1"]
}


resource "aws_iam_role" "github_actions" {
  name = "ocn-github-actions"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect    = "Allow"
        Principal = {
          Federated = aws_iam_openid_connect_provider.github.arn
        }
        Action = "sts:AssumeRoleWithWebIdentity"
        Condition = {
          # The `@<id>` segments are NOT a typo. This organisation has
          # GitHub's "immutable org/repo IDs in OIDC claims" setting
          # enabled, so the token's subject carries the numeric org and
          # repository IDs alongside their names:
          #
          #   repo:alphastreet-ai@333030073/ocn@1216920516:ref:refs/heads/main
          #
          # read verbatim from the CloudTrail record of the denied
          # AssumeRoleWithWebIdentity call. A plain
          # "repo:alphastreet-ai/ocn:ref:refs/heads/main" never matches
          # it, which is why every deploy failed from the rename date
          # onward even with the org name corrected.
          #
          # The IDs are immutable: they survive renaming the org or the
          # repo, which is the point of the setting - a renamed repo
          # cannot impersonate the old one. Matching on them is
          # therefore MORE robust than matching on names, and this
          # condition needs no edit if the org is ever renamed again.
          StringLike = {
            "token.actions.githubusercontent.com:sub" = "repo:alphastreet-ai@333030073/ocn@1216920516:ref:refs/heads/main"
          }
          # The audience the aws-actions/configure-aws-credentials action
          # requests. Unconditioned, any GitHub-minted token for this
          # provider could be presented; pinning it is standard practice
          # for this integration.
          StringEquals = {
            "token.actions.githubusercontent.com:aud" = "sts.amazonaws.com"
          }
        }
      }
    ]
  })
}


resource "aws_iam_role_policy" "github_actions_deploy" {
  name = "ocn-github-actions-deploy"
  role = aws_iam_role.github_actions.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "ecr:GetAuthorizationToken",
          "ecr:BatchCheckLayerAvailability",
          "ecr:PutImage",
          "ecr:InitiateLayerUpload",
          "ecr:UploadLayerPart",
          "ecr:CompleteLayerUpload"
        ]
        Resource = "*"
      },
      {
        Effect = "Allow"
        Action = [
          "ecs:DescribeTaskDefinition",
          "ecs:RegisterTaskDefinition",
          "ecs:UpdateService"
        ]
        Resource = "*"
      },
      {
        Effect   = "Allow"
        Action   = ["iam:PassRole"]
        Resource = [
          aws_iam_role.ecs_task_execution.arn,
          aws_iam_role.ecs_task_exec_ssm.arn
        ]
      }
    ]
  })
}
